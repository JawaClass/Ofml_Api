import os
import re
import datetime
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Dict
import pandas as pd


def catch_file_exception(f):
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except (OSError, IOError) as e:
            return NotAvailable(e)
        return result

    return wrapper


class NotAvailable:

    def __init__(self, error):
        self.error = error


class Repository:

    def __str__(self):
        return f"Repository {self.root} -> {self.__programs.items()}"

    def __init__(self, root: Path, **kwargs):
        self.root = root

        self.profiles = None
        self.__programs = OrderedDict()

    def programs(self):
        return self.__programs.values()

    def __getitem__(self, program) -> 'Program':
        return self.__programs[program]

    def read_profiles(self):
        self.profiles = ConfigFile(self.root / 'profiles' / 'kn.cfg')

    def read_registry(self, program):
        registry_name = self.program_name2registry_name(program)
        return ConfigFile(self.root / 'registry' / f'{registry_name}.cfg')

    def load_program(self, program, keep_in_memory: bool = True, program_cls=None):
        if program_cls is None:
            program_cls = Program
        reg = self.read_registry(program)
        program = program_cls(registry=reg, root=self.root)
        if keep_in_memory:
            self.__programs[program] = program
        return program

    def program_names(self):
        if self.profiles is None:
            raise ValueError("First read the profiles file")
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
            'ocd': self.root / self.registry['productdb_path'] if self.contains_ocd() else None,
            'oam': self.root / self.registry['oam_path'] if self.contains_oam() else None,
            'oap': self.program_path / 'DE' / '2' / 'oap' if self.contains_oap() else None,
            'oas': self.program_path / 'DE' / '2' / 'cat' if self.contains_oas() else None,
            'go': self.program_path / '2' if self.contains_go() else None
        }

        self.ocd: Optional[OFMLPart] = None
        self.oam: Optional[OFMLPart] = None
        self.go: Optional[OFMLPart] = None
        self.oas: Optional[OFMLPart] = None
        self.oap: Optional[OFMLPart] = None

    @property
    def all_tables(self):
        tables = []
        if self.is_ocd_available():
            for table in self.ocd.tables.values():
                if type(table) is Table:
                    tables.append(table)
        if self.is_oas_available():
            for table in self.oas.tables.values():
                if type(table) is Table:
                    tables.append(table)
        if self.is_oam_available():
            for table in self.oam.tables.values():
                if type(table) is Table:
                    tables.append(table)
        if self.is_go_available():
            for table in self.go.tables.values():
                if type(table) is Table:
                    tables.append(table)
        if self.is_oap_available():
            for table in self.oap.tables.values():
                if type(table) is Table:
                    tables.append(table)
        return tables

    def load_ocd(self):
        return self._read_ofml_part(ofml_part='ocd', inp_descr=self.paths['ocd'] / 'pdata.inp_descr')

    def load_oam(self):
        return self._read_ofml_part(ofml_part='oam', inp_descr=self.paths['oam'] / 'oam.inp_descr')

    def load_go(self):
        return self._read_ofml_part(ofml_part='go', inp_descr=self.paths['go'] / 'mt.inp_descr')

    def load_oap(self):
        return self._read_ofml_part(ofml_part='oap', inp_descr=self.paths['oap'] / 'oap.inp_descr')

    def load_oas(self):

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

        return self._read_ofml_part(ofml_part='oas', tables_definitions=tables_definitions, path=path)

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
        return type(self.ocd) is OFMLPart

    def is_oam_available(self):
        return type(self.oam) is OFMLPart

    def is_oas_available(self):
        return type(self.oas) is OFMLPart

    def is_go_available(self):
        return type(self.go) is OFMLPart

    def is_oap_available(self):
        return type(self.oap) is OFMLPart

    def load_all(self):
        if self.contains_ocd():
            self.load_ocd()
        if self.contains_oam():
            self.load_oam()
        if self.contains_oas():
            self.load_oas()
        if self.contains_go():
            self.load_go()
        if self.contains_oap():
            self.load_oap()

    def contains_ocd(self):
        return 'productdb_path' in self.registry

    def contains_oam(self):
        return 'oam_path' in self.registry

    def contains_go(self):
        return 'series_type' in self.registry and 'meta_type' in self.registry

    def contains_oap(self):
        oap_path = self.root / f'kn/{self.name}/DE/2/oap'
        return oap_path.exists()

    def contains_oas(self):
        return 'type' in self.registry and self.registry.get('cat_type', None) in ['XCF']

    def ofml_parts(self):
        return {
            'ocd': {'features': self.contains_ocd(), 'loaded': bool(self.ocd)},
            'oam': {'features': self.contains_oam(), 'loaded': bool(self.oam)},
            'go': {'features': self.contains_go(), 'loaded': bool(self.go)},
            'oas': {'features': self.contains_oas(), 'loaded': bool(self.oas)},
            'oap': {'features': self.contains_oap(), 'loaded': bool(self.oap)},
        }

    def featured_ofml_parts(self):
        return [_ for _, v in self.ofml_parts().items() if v['features'] is True]

    def _read_ofml_part(self, **kwargs):

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

        return self.__getattribute__(ofml_part)

    def __str__(self):
        return f'Program: {self.name}, OFML Parts: {self.ofml_parts()}'

    def __repr__(self):
        return self.__str__()


class TimestampFile:

    def __init__(self, path):
        self.path = path
        self._file_attributes = os.stat(self.path)
        timestamp = datetime.datetime.now()
        self.timestamp_read = timestamp.strftime("%Y-%m-%d-%H-%M-%S")

    def __str__(self):
        return f'TimestampFile: path={self.path}'

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
        with open(self.path, encoding="cp1252") as f:
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
                        d[section][k] = v
                    else:
                        d[k] = v
        return d


class Table(TimestampFile):

    def __init__(self, df: pd.DataFrame, filepath: Path):
        super().__init__(filepath)
        self.df: pd.DataFrame = df
        self.name = filepath.name
        self.database_table_name = re.sub(r"\..*$", "", self.name)

    def database_column_type(self, column_name):
        dtype = str(self.df[column_name].dtype)
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

    def read_all_tables(self):
        for name in self.filenames:
            self.read_table(name)

    def read_table(self, filename, encoding="cp1252"):#"#'ANSI'):
        table_path = self.path / filename
        columns, dtypes = self.tables_definitions[filename]
        dtypes = {_[0]: _[1] for _ in zip(columns, dtypes)}

        table = re.sub(r'\..+$', '', filename)
        self.tables[table] = read_table(table_path, columns, dtypes, encoding)
        return self.tables[table]

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
    except (ValueError, FileNotFoundError, ) as e:
        return NotAvailable(e)

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return Table(df, filepath)


def ofml_dtype_2_pandas_dtype(ofml_dtype):
    ofml_dtype = str.lower(ofml_dtype)
    if 'string' in ofml_dtype:
        return 'string'
    return ofml_dtype


@catch_file_exception
def read_pdata_inp_descr(file_name):
    result = OrderedDict()
    inside_comment = False
    with open(file_name, 'r', encoding="cp1252") as file:
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

                result[table_name][0].append(field_name)
                result[table_name][1].append(datatype)
    return result


