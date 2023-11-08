import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileSystemEvent
from watchdog.observers import Observer

from db import DB
from repository import Repository, Program, OFMLPart, read_pdata_inp_descr, NotAvailable
import logging

from ws_server import notify_clients, start_server_in_thread

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
            # print('Duplicate', event, 'last_time=', last_time)
            return
        # else:
        #     print('Dispatch', event, 'last_time=', last_time)
        super().dispatch(event)


class LiveOFMLPart(OFMLPart, FileSystemEventHandlerSingleEvent):

    @staticmethod
    def from_inp_descr(**kwargs):
        print('from_inp_descr....', kwargs)
        inp_descr_path = kwargs['inp_descr_path']
        callback = kwargs['callback']
        program = kwargs['program']
        tables_definitions = read_pdata_inp_descr(inp_descr_path)

        print('tables_definitions', tables_definitions)

        if isinstance(tables_definitions, NotAvailable):
            return tables_definitions

        path = inp_descr_path.parents[0]
        return LiveOFMLPart(path=path, tables_definitions=tables_definitions, callback=callback, program=program)

    @staticmethod
    def from_tables_definitions(**kwargs):
        path: Path = kwargs['path']
        callback = kwargs['callback']
        tables_definitions = kwargs['tables_definitions']
        program = kwargs['program']

        if not path.exists():
            return NotAvailable(None)

        return LiveOFMLPart(path=path, tables_definitions=tables_definitions, callback=callback, program=program)

    def __init__(self, callback, **kwargs):

        FileSystemEventHandlerSingleEvent.__init__(self)
        OFMLPart.__init__(self, **kwargs)

        self.on_change = callback
        self.listen_folder(path=str(self.path), observer=Observer())
        self._program = kwargs['program']

        self._ignore_event_tables = set()

    async def init_tables(self):
        print('init_table')
        print('filenames', self.filenames)
        for table_name in self.filenames:
            self.update_table(table_name)
            print(table_name)
            if self.is_table_available(table_name):
                print('table', table_name, 'available')
                await self.on_change(filename=table_name, ofml_part=self, event=None)

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
            if type(self.table(filename)) is not NotAvailable:
                self.on_change(filename=filename, ofml_part=self, event=event)
        except PermissionError:
            logger.warning(f'PermissionError: could not update table {filename} of {self.name}')
            pass


class LiveProgram(Program):

    def __init__(self, callback, observer, **kwargs):
        super().__init__(**kwargs)

        self._observer = observer

        self.ocd: Optional[LiveOFMLPart] = None
        self.oam: Optional[LiveOFMLPart] = None
        self.go: Optional[LiveOFMLPart] = None
        self.oas: Optional[LiveOFMLPart] = None
        self.oap: Optional[LiveOFMLPart] = None

        self.callback = callback

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

        # print('DF_COLUMNS', table.df.columns)
        #
        # with DB as db:
        #     c = db.cursor()
        #     import sqlite3
        #     rt = c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{sql_table_name}';")
        #     if rt.fetchone():
        #
        #         rt = c.execute(f"PRAGMA table_info([{sql_table_name}]);")
        #         print('PRAGMA....', rt.fetchall())
        #         for column in rt.fetchall():
        #             column_name = column[1]
        #
        # # if table exists in DB
        # #   for c in df_columns:
        # #       if c not in table_schema:
        # #           DB_table add column c

        table.df.to_sql(sql_table_name, DB, if_exists='append', method='multi', chunksize=1000)
        print(f'DONE: inserting {sql_table_name} of {self.name}')
        self.callback(self, ofml_part, filename, event)

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

    def on_callback(self, *args, **kwargs):
        print('LiveRepository :: on_callback', args, kwargs)
        # ws_send('ws111111111111111111111')

        message = json.dumps({
            'COMMAND': 'update',
            'program': args[0].name,
            'ofml_part': args[1].name,
            'table': args[2],
        })

        # notify_clients(message)
        asyncio.run(notify_clients(message))
        print('SENT !!!')
        print('message', message)

    async def load_program(self, program, keep_in_memory=False):
        if keep_in_memory:

            if program in self.__programs:
                return self.__programs[program]
            else:
                reg = self.read_registry(program)
                self.__programs[program] = LiveProgram(registry=reg, root=self.root, callback=self.on_callback,
                                                       observer=Observer())
            return self.__programs[program]

        else:

            reg = self.read_registry(program)
            return LiveProgram(registry=reg, root=self.root, callback=self.on_callback,
                               observer=Observer())

    @classmethod
    async def create(cls, root: Path):
        repo = LiveRepository(root)

        for name in repo.program_names():

            if name not in ['talos', 's6', 'desks_m_cat']:  # 's6',
                continue

            program: LiveProgram = await repo.load_program(name, keep_in_memory=False)

            if program.has_ocd():
                print('load ocd')
                await program.load_ocd()

                # if program.is_ocd_available():
                #     await program.ocd.init_tables()

        return repo

    def __init__(self, root: Path):
        # super().__init__(root, **kwargs)

        FileSystemEventHandlerSingleEvent.__init__(self)
        Repository.__init__(self, root)

        self.listen_folder(path=str(self.root / 'profiles'), observer=Observer())

        self.read_profiles()

        # !!!!!!!!!!!!
        ###start_server_in_thread()

        # for name in self.program_names():
        #
        #     # if name not in ['talos', 's6', 'desks_m_cat']:  # 's6',
        #     #     continue
        #
        #     program: LiveProgram = self.load_program(name, keep_in_memory=False)
        #
        #     if program.has_ocd():
        #         print('load ocd')
        #         await program.load_ocd()

        # if program.is_ocd_available():
        #     program.ocd.init_tables()

        # if program.has_oas():
        #     program.load_oas()
        #     if program.is_oas_available():
        #         program.oas.init_tables()
        #
        # if program.has_oam():
        #     program.load_oam()
        #     if program.is_oam_available():
        #         program.oam.init_tables()
        #
        # if program.has_oap():
        #     program.load_oap()
        #     if program.is_oap_available():
        #         program.oap.init_tables()
        #
        # if program.has_go():
        #     program.load_go()
        #     if program.is_go_available():
        #         program.go.init_tables()


# with DB as db:
#     db.execute("ALTER TABLE [oap_propedit.csv] ADD COLUMN staterestr TEXT;")
#     db.commit()
#     input('..')

PATH = Path(r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics')

# start ws

# go live
loop = asyncio.get_event_loop()
# start_server_in_thread(loop)
loop.run_until_complete(LiveRepository.create(root=PATH))
loop.run_forever()

print('while loop ...')
while 1:
    ...
