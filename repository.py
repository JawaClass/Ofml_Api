import os
import re
import time
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.NOTSET)


def catch_file_exception(f):
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except (OSError, IOError) as e:
            print('.......catch_file_exception', args, kwargs)
            return NotAvailable(e)
            # input('.')
        return result

    return wrapper


class NotAvailable:

    def __init__(self, error):
        self.error = error


class Repository:

    def __init__(self, root: Path, **kwargs):
        self.root = root

        self.profiles = None
        self.__programs = OrderedDict()

    def __getitem__(self, program):
        return self.load_program(program)

    def read_profiles(self):
        self.profiles = ConfigFile(self.root / 'profiles' / 'kn.cfg')

    def read_registry(self, program):
        registry_name = self.program_name2registry_name(program)
        return ConfigFile(self.root / 'registry' / f'{registry_name}.cfg')

    def load_program(self, program, keep_in_memory=False):
        if keep_in_memory:

            if program in self.__programs:
                return self.__programs[program]
            else:
                reg = self.read_registry(program)
                self.__programs[program] = Program(registry=reg, root=self.root)
            return self.__programs[program]

        else:

            reg = self.read_registry(program)
            return Program(registry=reg, root=self.root)

    def program_names(self):
        return ['_'.join(cfg.split('_')[1:-2]) for cfg, active in self.profiles.config['[lib:kn]'].items() if active]

    def program_name2registry_name(self, program):
        for k, v in self.profiles.config['[lib:kn]'].items():

            profile_entry = '_'.join(k.split('_')[1:-2])

            if program == profile_entry:
                return k
        raise NotImplementedError(f'Program {program} has no registry entry in profiles')


class Program:

    def __init__(self, **kwargs):
        self.registry: ConfigFile = kwargs['registry']
        self.root: Path = kwargs['root']
        self.name: str = self.registry['program']

        self.program_path = self.root / 'kn' / self.name

        self.paths = {
            'ocd': self.root / self.registry['productdb_path'] if self.has_ocd() else None,
            'oam': self.root / self.registry['oam_path'] if self.has_oam() else None,
            'oap': self.program_path / 'DE' / '2' / 'oap' if self.has_oap() else None,
            'oas': self.program_path / 'DE' / '2' / 'cat' if self.has_oas() else None,
            'go': self.program_path / '2' if self.has_go() else None
        }

        self.ocd: Optional[OFMLPart] = None
        self.oam: Optional[OFMLPart] = None
        self.go: Optional[OFMLPart] = None
        self.oas: Optional[OFMLPart] = None
        self.oap: Optional[OFMLPart] = None

    def __str__(self):
        return f'Program: {self.name}, OFML Parts: {self.ofml_parts()}'

    def __repr__(self):
        return self.__str__()

    def read_ofml_part(self, **kwargs):

        inp_descr = kwargs.get('inp_descr', None)
        tables_definitions = kwargs.get('tables_definitions', None)

        assert inp_descr is None or tables_definitions is None
        assert inp_descr is not None or tables_definitions is not None

        ofml_part = kwargs['ofml_part']

        if inp_descr:
            self.__setattr__(ofml_part, OFMLPart.from_inp_descr(inp_descr))

        else:
            path = kwargs['path']
            self.__setattr__(ofml_part, OFMLPart.from_tables_definitions(tables_definitions, path))

    def load_ofml_part(self, ofml_part):
        if ofml_part == 'ocd':
            self.load_ocd()
        if ofml_part == 'oam':
            self.load_oam()
        if ofml_part == 'go':
            self.load_go()
        if ofml_part == 'oap':
            self.load_oap()
        if ofml_part == 'oas':
            self.load_oas()

    def is_ocd_available(self):
        return type(self.ocd) is not NotAvailable

    def is_oam_available(self):
        return type(self.oam) is not NotAvailable

    def is_oas_available(self):
        return type(self.oas) is not NotAvailable

    def is_go_available(self):
        return type(self.go) is not NotAvailable

    def is_oap_available(self):
        return type(self.oap) is not NotAvailable

    async def load_ocd(self):
        self.read_ofml_part(ofml_part='ocd', inp_descr=self.paths['ocd'] / 'pdata.inp_descr')

    async def load_oam(self):
        self.read_ofml_part(ofml_part='oam', inp_descr=self.paths['oam'] / 'oam.inp_descr')

    async def load_go(self):
        self.read_ofml_part(ofml_part='go', inp_descr=self.paths['go'] / 'mt.inp_descr')

    async def load_oap(self):
        self.read_ofml_part(ofml_part='oap', inp_descr=self.paths['oap'] / 'oap.inp_descr')

    async def load_oas(self):

        path = self.paths['oas']
        tables_definitions = OrderedDict(**{
            'article.csv': [['name', 'type', 'param3', 'param4', 'param5', 'param6', 'program'],
                            ['string', 'string', 'string', 'string', 'string', 'string', 'string']],
            'resource.csv': [['name', 'type', 'param3', 'param4', 'resource_path'],
                             ['string', 'string', 'string', 'string', 'string', ]],

            'structure.csv': [['name', 'type', 'param3', 'param4', 'param5'],
                              ['string', 'string', 'string', 'string', 'string', ]],

            'text.csv': [['name', 'type', 'language', 'text'],
                         ['string', 'string', 'string', 'string']],

        })

        self.read_ofml_part(ofml_part='oas', tables_definitions=tables_definitions, path=path)

    def has_ocd(self):
        return 'productdb_path' in self.registry

    def has_oam(self):
        return 'oam_path' in self.registry

    def has_go(self):
        return 'series_type' in self.registry and 'meta_type' in self.registry

    def has_oap(self):
        oap_path = self.root / f'kn/{self.name}/DE/2/oap'
        return oap_path.exists()

    def has_oas(self):
        return 'type' in self.registry and self.registry.get('cat_type', None) in ['XCF']

    def ofml_parts(self):
        return {
            'ocd': {'features': self.has_ocd(), 'loaded': bool(self.ocd)},
            'oam': {'features': self.has_oam(), 'loaded': bool(self.oam)},
            'go': {'features': self.has_go(), 'loaded': bool(self.go)},
            'oas': {'features': self.has_oas(), 'loaded': bool(self.oas)},
            'oap': {'features': self.has_oap(), 'loaded': bool(self.oap)},
        }

    def featured_ofml_parts(self):
        return [_ for _, v in self.ofml_parts().items() if v['features'] is True]


class TimestampFile:

    def __init__(self, path):
        self.path = path
        self._file_attributes = os.stat(self.path)
        self.timestamp_read = time.time()

    def __str__(self):
        return f'TimestampFile: path={self.path}, timestamp_modified={self.timestamp_modified}, timestamp_read={self.timestamp_read}'

    def __repr__(self):
        return self.__str__()

    @property
    def timestamp_modified(self) -> float:
        return self._file_attributes.st_mtime

    def is_newer(self, other: 'TimestampFile'):
        return self.timestamp_modified > other.timestamp_modified

    def is_older(self, other: 'TimestampFile'):
        return self.timestamp_modified < other.timestamp_modified


class ConfigFile(TimestampFile):

    def __init__(self, path: Path):
        super().__init__(path)
        self.path = path
        self.config = self.read()

    def __iter__(self):
        return iter(self.config)

    def __getitem__(self, item):
        return self.config[item]

    def get(self, *args, **kwargs):
        return self.config.get(*args, **kwargs)

    def read(self):
        with open(self.path) as f:
            section = None
            d = OrderedDict()
            for _ in f.readlines():
                _ = _.strip()
                if not _ or _.isspace() or _.startswith('#'):
                    continue
                if re.match(r'\[.+]', _):
                    section = _
                    d[section] = OrderedDict()

                if re.match('.+=.+', _):
                    k, v = _.split('=')
                    if section:
                        # print('section', section, k, v)
                        d[section][k] = v
                    else:
                        d[k] = v
        return d


class Table(TimestampFile):

    def __init__(self, df: pd.DataFrame, filepath: Path):
        super().__init__(filepath)
        self.df: pd.DataFrame = df
        self.name = filepath.name
        self.database_table_name = self.name.replace('.', '_')

    def database_column_type(self, column_name):
        dtype = str(self.df[column_name].dtype)
        print('database_column_type', column_name, dtype, type(dtype))
        return {
            'string': 'varchar(255)',
            'float64': 'float',
            'int': 'integer',
        }[dtype]


class OFMLPart:
    """
    this class is for reading any ofml data
    """

    @staticmethod
    def from_inp_descr(inp_descr_path):
        tables_definitions = read_pdata_inp_descr(inp_descr_path)
        if isinstance(tables_definitions, NotAvailable):
            return tables_definitions
        path = inp_descr_path.parents[0]
        return OFMLPart(path=path, tables_definitions=tables_definitions)

    @staticmethod
    def from_tables_definitions(tables_definitions, path):
        return OFMLPart(path=path, tables_definitions=tables_definitions)

    def __init__(self, **kwargs):
        self.path: Path = kwargs['path']
        self.name = {
            'db': 'ocd',
            'oap': 'oap',
            'oam': 'oam',
            '2': 'go',
            'cat': 'oas',
        }[self.path.name]
        self.tables_definitions = kwargs['tables_definitions']
        self.tables: Dict[str, Table] = OrderedDict()

    @property
    def filepaths(self):
        return {self.path / _ for _ in self.tables_definitions.keys()}

    @property
    def filenames(self):
        return {_ for _ in self.tables_definitions.keys()}

    def __repr__(self):
        return f'OFMLPart name = {self.name}'

    def read_table(self, table, encoding='ANSI'):
        # print('read_table', table)
        table_path = self.path / table
        columns, dtypes = self.tables_definitions[table]
        dtypes = {_[0]: _[1] for _ in zip(columns, dtypes)}

        table = re.sub(r'\..+$', '', table)
        self.tables[table] = read_table(table_path, columns, dtypes, encoding)

    def table(self, name: str) -> Table:
        name = re.sub(r'\..+$', '', name)
        return self.tables[name]

    def is_table_available(self, name):
        return type(self.table(name)) is not NotAvailable

    def __getitem__(self, item):
        return self.table(item)


def read_table(filepath, names, dtype, encoding):
    """
    given a filepath and the names from inp_descr read any table
    """
    on_bad_lines = 'warn'  # warn, skip, error
    try:
        df = pd.read_csv(filepath, sep=';',
                         header=None,
                         names=names,
                         dtype=dtype,
                         encoding=encoding,
                         comment='#',
                         on_bad_lines=on_bad_lines)
    except ValueError as e:
        print('read_table ERR', e)
        return NotAvailable(e)
    return Table(df, filepath)


def ofml_dtype_2_pandas_dtype(ofml_dtype):
    ofml_dtype = str.lower(ofml_dtype)
    if 'string' in ofml_dtype:
        return 'string'
    # if 'int' in ofml_dtype:
    #     return 'Int64'
    return ofml_dtype


@catch_file_exception
def read_pdata_inp_descr(file_name):
    result = OrderedDict()
    inside_comment = False
    with open(file_name, 'r') as file:
        for line in file:

            if line.isspace():
                continue

            row = line.split()

            if row[0] == 'comment':
                inside_comment = True

            if len(row) >= 2 and f'{row[0]} {row[1]}' == 'end comment':
                inside_comment = False

            if inside_comment:
                continue

            if row[0] == 'table':
                table_name = row[2]
                result[table_name] = [[], []]
            elif row[0] == 'field':
                field_name = row[2]
                datatype = row[3]
                datatype = ofml_dtype_2_pandas_dtype(datatype)

                # result[table_name].append({'field': field_name, 'datatype': datatype})
                result[table_name][0].append(field_name)
                result[table_name][1].append(datatype)
    return result


#
#
# # Press the green button in the gutter to run the script.
if __name__ == '__main__':
    string = 'methodCall("AC_MC_WIDTH")*0.5-0.1 + methodCall("AC_MC_HEIGHT")'

    res = re.findall('methodCall\("(.+?)"\)', string)

    print(res)
    print(type(res))

    print('---')

    res = re.search('methodCall\(.+?\)', string)

    print(res)
    print(type(res))
#
#     i = 8329
#     j = 8192
#
#     print(i & j)
#     print(i & 8)
#     repo = Repository(Path(r'b:\Testumgebung\EasternGraphics'))
#
#     for name, program in repo.programs.items():
#         print(program.name)
#         print(program.features)
#         print('...')
#
