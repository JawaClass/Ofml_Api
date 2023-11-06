import threading
import time
from collections import defaultdict
from pathlib import Path
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileSystemEvent
from watchdog.observers import Observer

from db import DB
from repository import Repository, Program, OFMLPart, read_pdata_inp_descr
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.NOTSET)


class FileSystemEventHandlerSingleEvent(FileSystemEventHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # src_path: {event_type: timestamp}
        self._timestamps = defaultdict(dict)

    def listen_folder(self, path, observer):
        print('start listening:', path)
        observer.schedule(path=path, recursive=False, event_handler=self)
        observer.start()

    def dispatch(self, event: FileSystemEvent):
        now = time.time()

        last_time = self._timestamps[event.src_path].get(event.event_type, None)
        self._timestamps[event.src_path][event.event_type] = time.time()

        if last_time is None:
            # rint('dispatch', event)
            return super().dispatch(event)

        time_delta = abs(now - last_time)
        threshold = 0.2
        is_duplicate = (last_time is not None and time_delta < threshold)
        if is_duplicate:
            return

        super().dispatch(event)


class LiveOFMLPart(OFMLPart, FileSystemEventHandlerSingleEvent):

    def __init__(self, callback, **kwargs):

        FileSystemEventHandlerSingleEvent.__init__(self)
        OFMLPart.__init__(self, **kwargs)

        self.on_change = callback
        self.listen_folder(path=str(self.path), observer=Observer())
        self._program = kwargs['program']

        self._ignore_event_tables = set()

    @staticmethod
    def from_inp_descr(**kwargs):
        inp_descr_path = kwargs['inp_descr_path']
        callback = kwargs['callback']
        program = kwargs['program']
        tables_definitions = read_pdata_inp_descr(inp_descr_path)
        path = inp_descr_path.parents[0]
        return LiveOFMLPart(path=path, tables_definitions=tables_definitions, callback=callback, program=program)

    @staticmethod
    def from_tables_definitions(**kwargs):
        path = kwargs['path']
        callback = kwargs['callback']
        tables_definitions = kwargs['tables_definitions']
        program = kwargs['program']

        print(kwargs)

        return LiveOFMLPart(path=path, tables_definitions=tables_definitions, callback=callback, program=program)

    def update_table(self, table_name):
        # reading the table will trigger a modification event we will ignore
        self._ignore_event_tables.add(table_name)
        self.read_table(table_name)

    def on_any_event(self, event: FileModifiedEvent):
        filename = Path(event.src_path).name

        if filename in self._ignore_event_tables:
            self._ignore_event_tables.remove(filename)
            return

        if not self.filenames:
            # Should not happen currently
            logger.warning(
                f'LiveOFMLPart {self.name} received event but was not initialized (Should not happen currently)')
        if filename not in self.filenames:
            return

        try:
            self.update_table(filename)
            self.on_change(filename=filename, ofml_part=self, event=event)
        except PermissionError:
            logger.warning(f'PermissionError: could not update table {self.filenames} of {self.name}')
            pass


class LiveProgram(Program):

    def __init__(self, callback, observer, **kwargs):
        super().__init__(**kwargs)

        # self.on_program_change = callback
        self._observer = observer
        # self._observer.schedule(self, path=self.program_path, recursive=False)

    def on_ofml_part_change(self, filename, ofml_part: LiveOFMLPart, event):
        print('ofml_part_change', filename, self.name, ofml_part, event)
        table = ofml_part.table(filename)
        sql_table_name = str(table.name)  # linter
        table.df['__sql__program__'] = self.name
        table.df['__sql__timestamp_modified__'] = table.timestamp_modified
        table.df['__sql__timestamp_read__'] = table.timestamp_read

        # drop table entries for program if exists
        with DB as db:
            c = db.cursor()
            import sqlite3
            try:
                # if table exists...
                rt = c.execute(f"DELETE FROM [{sql_table_name}] WHERE __sql__program__='{self.name}';")
                # print('Delete::', rt.fetchall())
            except sqlite3.OperationalError:
                pass
        print(f'START: inserting {sql_table_name} of {self.name}')
        table.df.to_sql(sql_table_name, DB, if_exists='append', method='multi', chunksize=1000)
        print(f'DONE: inserting {sql_table_name} of {self.name}')

    def read_ofml_part(self, **kwargs):

        inp_descr = kwargs.get('inp_descr', None)
        tables_definitions = kwargs.get('tables_definitions', None)

        assert inp_descr is None or tables_definitions is None

        ofml_part = kwargs['ofml_part']

        if inp_descr:
            self.__setattr__(ofml_part,
                             LiveOFMLPart.from_inp_descr(inp_descr_path=inp_descr,
                                                         callback=self.on_ofml_part_change,
                                                         program=self))

        else:
            path = kwargs['path']
            assert path is not None
            self.__setattr__(ofml_part,
                             LiveOFMLPart.from_tables_definitions(tables_definitions=tables_definitions,
                                                                  path=path,
                                                                  callback=self.on_ofml_part_change,
                                                                  program=self))


class LiveRepository(Repository, FileSystemEventHandlerSingleEvent):

    # def dispatch(self, event):
    #     print('LiveRepository . dispatch', event)

    def load_program(self, program, keep_in_memory=False):
        if keep_in_memory:

            if program in self.__programs:
                return self.__programs[program]
            else:
                reg = self.read_registry(program)
                self.__programs[program] = LiveProgram(registry=reg, root=self.root, callback=None,
                                                       observer=Observer())
            return self.__programs[program]

        else:

            reg = self.read_registry(program)
            return LiveProgram(registry=reg, root=self.root, callback=None,
                               observer=Observer())

    def __init__(self, root: Path, **kwargs):
        # super().__init__(root, **kwargs)

        FileSystemEventHandlerSingleEvent.__init__(self)
        Repository.__init__(self, root)

        self.listen_folder(path=str(self.root / 'profiles'), observer=Observer())

        self.read_profiles()

        self._watched_files = set()
        self._watched_file2program = {}

        for name in self.program_names():

            # if not name == 'desks_m_cat':
            #     continue

            program: LiveProgram = self.load_program(name, keep_in_memory=False)

            if program.has_ocd():
                print('load_ocd', name, program)
                program.load_ocd()
                for table_name in program.ocd.tables:
                    program.ocd.update_table(table_name)  # LiveOFMLPart
                    program.on_ofml_part_change(filename=table_name, ofml_part=program.ocd, event=None)

            if program.has_oas():
                # print('load_oas', name, program)
                program.load_oas()

            if program.has_oam():
                # print('load_oas', name, program)

                program.load_oam()


PATH = Path(r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics')
live_repo = LiveRepository(root=PATH)

print(live_repo.root)
print(live_repo.profiles)

while 1:
    ...
