import csv
import os
import re
import datetime
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Dict, Union
import pandas as pd
from ofml_api.util import NotAvailable, catch_file_exception


class Repository:

    def __init__(self, root: Path, manufacturer: str):
        self.root = root if isinstance(root, Path) else Path(root)
        self.manufacturer = manufacturer
        self.profiles = None
        self.__programs = OrderedDict()

    def __str__(self):
        return f"Repository(root={self.root}, programs={self.__programs.items()})"

    def programs(self):
        return self.__programs.values()

    def __getitem__(self, program) -> "Program":
        return self.__programs[program]

    def read_profiles(self):
        self.profiles = ConfigFile(self.root / "profiles" / f"{self.manufacturer}.cfg")

    def __read_registry(self, program):
        try:
            registry_name = self.program_name2registry_name(program)
            return ConfigFile(self.root / "registry" / f"{registry_name}.cfg")
        except (
            ValueError,
            FileNotFoundError,
        ) as e:
            return NotAvailable(e)

    def load_program(
        self,
        program_name: str,
        keep_in_memory: bool = True,
        region: Optional[str] = None,
    ) -> Union["Program", NotAvailable]:
        reg = self.__read_registry(program_name)

        if isinstance(reg, NotAvailable):
            return reg

        if region is None:
            region = reg.get("distribution_region").strip()

        program = Program(
            registry=reg, root=self.root, manufacturer=self.manufacturer, region=region
        )
        if keep_in_memory:
            self.__programs[program_name] = program
        return program

    def program_names(self):
        if self.profiles is None:
            raise ValueError("First read the profiles file")
        return [
            "_".join(cfg.split("_")[1:-2])
            for cfg, active in self.profiles.config[
                f"[lib:{self.manufacturer}]"
            ].items()
            if active
        ]

    def program_name2registry_name(self, program):
        for k in self.profiles.config[f"[lib:{self.manufacturer}]"]:
            profile_entry = "_".join(k.split("_")[1:-2])
            if profile_entry == program:
                return k
        raise ValueError(f"Program {program} has no registry entry in profiles")


class Program:

    def __init__(
        self, registry: "ConfigFile", root: Path, manufacturer: str, region: str
    ):
        self.registry: ConfigFile = registry
        self.root: Path = root
        self.name: str = self.registry["program"]
        self.manufacturer = manufacturer
        self.region = region
        self.program_path = self.root / self.manufacturer / self.name
        self.paths = {
            "ocd": (
                self.root / self.registry["productdb_path"]
                if self.contains_ofml_part("ocd")
                else None
            ),
            "oam": (
                self.root / self.registry["oam_path"]
                if self.contains_ofml_part("oam")
                else None
            ),
            "oap": (
                self.program_path / self.region / "2" / "oap"
                if self.contains_ofml_part("oap")
                else None
            ),
            "oas": (
                self.program_path / self.region / "2" / "cat"
                if self.contains_ofml_part("oas")
                else None
            ),
            "go": self.program_path / "2" if self.contains_ofml_part("go") else None,
            "odb": self.program_path / "2" if self.contains_ofml_part("odb") else None,
        }

        self.parts: Dict[str, Optional[OFMLPart]] = {
            "ocd": None,
            "oam": None,
            "go": None,
            "oas": None,
            "oap": None,
            "odb": None,
        }

    @property
    def ocd(self):
        return self.parts["ocd"]

    @ocd.setter
    def ocd(self, value):
        self.parts["ocd"] = value

    @property
    def oam(self):
        return self.parts["oam"]

    @oam.setter
    def oam(self, value):
        self.parts["oam"] = value

    @property
    def go(self):
        return self.parts["go"]

    @go.setter
    def go(self, value):
        self.parts["go"] = value

    @property
    def oas(self):
        return self.parts["oas"]

    @oas.setter
    def oas(self, value):
        self.parts["oas"] = value

    @property
    def oap(self):
        return self.parts["oap"]

    @oap.setter
    def oap(self, value):
        self.parts["oap"] = value

    @property
    def odb(self):
        return self.parts["odb"]

    @odb.setter
    def odb(self, value):
        self.parts["odb"] = value

    def all_tables(self):
        tables = []
        if self.is_ofml_part_available("ocd"):
            tables.extend(
                [
                    table
                    for table in self.ocd.tables.values()
                    if isinstance(table, Table)
                ]
            )
        if self.is_ofml_part_available("oas"):
            tables.extend(
                [
                    table
                    for table in self.oas.tables.values()
                    if isinstance(table, Table)
                ]
            )
        if self.is_ofml_part_available("oam"):
            tables.extend(
                [
                    table
                    for table in self.oam.tables.values()
                    if isinstance(table, Table)
                ]
            )
        if self.is_ofml_part_available("go"):
            tables.extend(
                [table for table in self.go.tables.values() if isinstance(table, Table)]
            )
        if self.is_ofml_part_available("oap"):
            tables.extend(
                [
                    table
                    for table in self.oap.tables.values()
                    if isinstance(table, Table)
                ]
            )
        if self.is_ofml_part_available("odb"):
            tables.extend(
                [
                    table
                    for table in self.odb.tables.values()
                    if isinstance(table, Table)
                ]
            )
        return tables

    def load_ofml_part(self, ofml_part: str, *args, **kwargs) -> "OFMLPart":
        return {
            "ocd": self.__load_ocd,
            "oam": self.__load_oam,
            "go": self.__load_go,
            "oap": self.__load_oap,
            "oas": self.__load_oas,
            "odb": self.__load_odb,
        }[ofml_part](*args, **kwargs)

    def contains_ofml_part(self, ofml_part: str, *args, **kwargs) -> bool:
        return {
            "ocd": self.__contains_ocd,
            "oam": self.__contains_oam,
            "go": self.__contains_go,
            "oap": self.__contains_oap,
            "oas": self.__contains_oas,
            "odb": self.__contains_odb,
        }[ofml_part](*args, **kwargs)

    def is_ofml_part_available(self, ofml_part: str, *args, **kwargs) -> bool:
        return {
            "ocd": self.__is_ocd_available,
            "oam": self.__is_oam_available,
            "go": self.__is_go_available,
            "oap": self.__is_oap_available,
            "oas": self.__is_oas_available,
            "odb": self.__is_odb_available,
        }[ofml_part](*args, **kwargs)

    def load_all_ofml_parts(self):
        for part in self.parts:
            if self.contains_ofml_part(part):
                self.load_ofml_part(part)

    def __is_ocd_available(self):
        return isinstance(self.ocd, OFMLPart)

    def __is_oam_available(self):
        return isinstance(self.oam, OFMLPart)

    def __is_oas_available(self):
        return isinstance(self.oas, OFMLPart)

    def __is_go_available(self):
        return isinstance(self.go, OFMLPart)

    def __is_odb_available(self):
        return isinstance(self.odb, OFMLPart)

    def __is_oap_available(self):
        return isinstance(self.oap, OFMLPart)

    def __contains_ocd(self):
        return "productdb_path" in self.registry

    def __contains_oam(self):
        return "oam_path" in self.registry

    def __contains_go(self):
        return "series_type" in self.registry and "meta_type" in self.registry

    def __contains_odb(self):
        odb_path = self.root / f"{self.manufacturer}/{self.name}/2"
        return odb_path.exists()

    def __contains_oap(self):
        oap_path = self.root / f"{self.manufacturer}/{self.name}/{self.region}/2/oap"
        return oap_path.exists()

    def __contains_oas(self):
        return "type" in self.registry and self.registry.get("cat_type", None) in [
            "XCF"
        ]

    def __load_ocd(self):
        return self._read_ofml_part(
            ofml_part="ocd", inp_descr=self.paths["ocd"] / "pdata.inp_descr", name="ocd"
        )

    def __load_oam(self):
        return self._read_ofml_part(
            ofml_part="oam", inp_descr=self.paths["oam"] / "oam.inp_descr", name="oam"
        )

    def __load_odb(self):
        return self._read_ofml_part(
            ofml_part="odb", inp_descr=self.paths["odb"] / "odb.inp_descr", name="odb"
        )

    def __load_go(self, languages=["de", "en", "fr", "nl"]):
        ofml_part = self._read_ofml_part(
            ofml_part="go", inp_descr=self.paths["go"] / "mt.inp_descr", name="go"
        )

        if not isinstance(ofml_part, NotAvailable):
            for language in languages:
                ofml_part.tables_definitions[f"{self.name}_{language}.sr"] = [
                    ["key", "value"],
                    ["string", "string"],
                    "=",
                ]
        return ofml_part

    def __load_oap(self):
        return self._read_ofml_part(
            ofml_part="oap", inp_descr=self.paths["oap"] / "oap.inp_descr", name="oap"
        )

    def __load_oas(self):

        path = self.paths["oas"]
        tables_definitions = OrderedDict(
            **{
                "article.csv": [
                    ["name", "type", "param3", "param4", "param5", "param6", "program"],
                    [
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                    ],
                    ";",
                ],
                "resource.csv": [
                    ["name", "type", "param3", "param4", "resource_path"],
                    [
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                    ],
                    ";",
                ],
                "structure.csv": [
                    ["name", "type", "param3", "param4", "param5"],
                    [
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                    ],
                    ";",
                ],
                "text.csv": [
                    ["name", "type", "language", "text"],
                    ["string", "string", "string", "string"],
                    ";",
                ],
            }
        )

        return self._read_ofml_part(
            ofml_part="oas",
            tables_definitions=tables_definitions,
            path=path,
            name="oas",
        )

    def ofml_parts(self):
        return {
            part: {
                "features": self.contains_ofml_part(part),
                "loaded": bool(self.parts[part]),
            }
            for part in self.parts
        }

    def featured_ofml_parts(self):
        return [_ for _, v in self.ofml_parts().items() if v["features"] is True]

    def _read_ofml_part(self, **kwargs) -> "OFMLPart":

        inp_descr = kwargs.get("inp_descr", None)
        tables_definitions = kwargs.get("tables_definitions", None)

        assert inp_descr is None or tables_definitions is None
        assert inp_descr is not None or tables_definitions is not None

        ofml_part = kwargs["ofml_part"]

        if inp_descr:
            self.__setattr__(
                ofml_part, OFMLPart.from_inp_descr(inp_descr, kwargs["name"])
            )

        else:
            path = kwargs["path"]
            self.__setattr__(
                ofml_part,
                OFMLPart.from_tables_definitions(
                    tables_definitions, path, kwargs["name"]
                ),
            )

        return self.__getattribute__(ofml_part)

    def __str__(self):
        return f"Program(name={self.name}, ofml_parth={self.ofml_parts()})"

    def __repr__(self):
        return self.__str__()


class TimestampFile:

    def __init__(self, path):
        self.path = path
        self._file_attributes = os.stat(self.path)
        timestamp = datetime.datetime.now()
        self.timestamp_read = timestamp.strftime("%Y-%m-%d-%H-%M-%S")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"TimestampFile(path={self.path})"

    @property
    def timestamp_modified(self) -> float:
        return self._file_attributes.st_mtime

    def is_newer(self, other: "TimestampFile"):
        return self.timestamp_modified > other.timestamp_modified

    def is_older(self, other: "TimestampFile"):
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

    def get(self, *args, **kwargs) -> Optional[str]:
        return self.config.get(*args, **kwargs)

    def read(self):
        with open(self.path, encoding="cp1252") as f:
            section = None
            d = OrderedDict()
            for _ in f.readlines():
                _ = _.strip()
                if not _ or _.isspace() or _.startswith("#"):
                    continue
                if re.match(r"\[.+]", _):
                    section = _
                    d[section] = OrderedDict()

                if re.match(".+=.+", _):
                    k, v = _.split("=")
                    k = k.strip()
                    v = v.strip()
                    if section:
                        d[section][k] = v
                    else:
                        d[k] = v
        return d


class Table(TimestampFile):

    def __init__(self, df: pd.DataFrame, filepath: Path, ofml_part_name: str):
        super().__init__(filepath)
        self.df: pd.DataFrame = df
        self.name: str = filepath.name
        self.ofml_part_name: str = ofml_part_name
        if re.search(r"(de|en|fr|nl)\.sr$", self.name):
            language = self.name[-5:-3]
            self.database_table_name = f"go_{language}_sr"
        else:
            self.database_table_name = re.sub(r"\..*$", "", self.name)

    def database_column_type(self, column_name):
        dtype = str(self.df[column_name].dtype)
        return {
            "string": "varchar(255)",
            "float64": "float",
            "int": "integer",
        }[dtype]


class OFMLPart:
    """
    this class is for reading any ofml data
    """

    @staticmethod
    def from_inp_descr(inp_descr_path, name):
        tables_definitions = read_pdata_inp_descr(inp_descr_path)
        if isinstance(tables_definitions, NotAvailable):
            return tables_definitions
        path = inp_descr_path.parents[0]
        return OFMLPart(path=path, tables_definitions=tables_definitions, name=name)

    @staticmethod
    def from_tables_definitions(tables_definitions, path, name):
        return OFMLPart(path=path, tables_definitions=tables_definitions, name=name)

    def __init__(self, **kwargs):
        self.path: Path = kwargs["path"]
        self.name = kwargs["name"]
        self.tables_definitions = kwargs["tables_definitions"]
        self.tables: Dict[str, Union[Table, NotAvailable]] = OrderedDict()

    @property
    def filepaths_from_tables_definitions(self):
        return {self.path / _ for _ in self.tables_definitions.keys()}

    @property
    def filenames_from_tables_definitions(self):
        return {_ for _ in self.tables_definitions.keys()}

    def __repr__(self):
        return f"OFMLPart(name={self.name}, path={self.path})"

    def read_all_tables(self):
        # TODO: sr files will be overwritten with ";" seperator...
        for name in self.filenames_from_tables_definitions:
            self.read_table(name)

    def read_table(
        self, filename: str, encoding="cp1252"
    ) -> Union[Table, NotAvailable]:
        try:
            columns, dtypes, sep = self.tables_definitions[filename]
        except KeyError:
            return NotAvailable(
                KeyError(f"key '{filename}' does not exist in tables_definitions")
            )
        table_path = self.path / filename
        dtypes = {_[0]: _[1] for _ in zip(columns, dtypes)}
        table = re.sub(r"\..+$", "", filename)
        quoting = csv.QUOTE_MINIMAL
        # most tables we can remove the enclosing " but not in these
        if table in {"funcs", "odb2d", "odb3d"}:
            quoting = csv.QUOTE_NONE
        self.tables[table] = read_table(
            table_path,
            columns,
            dtypes,
            encoding,
            sep=sep,
            quoting=quoting,
            ofml_part_name=self.name,
        )
        return self.tables[table]

    def table(self, name: str) -> Union[Table, NotAvailable]:
        name = re.sub(r"\..+$", "", name)
        return self.tables[name]

    def is_table_available(self, name: str):
        return type(self.table(name)) is not NotAvailable

    def __getitem__(self, item):
        return self.table(item)


def read_table(
    filepath, names, dtype, encoding, ofml_part_name, sep=";", quoting=csv.QUOTE_MINIMAL
):
    """
    given a filepath and the names from inp_descr read any table
    """
    from pandas._libs.parsers import STR_NA_VALUES

    na_values = STR_NA_VALUES - {"None"}

    on_bad_lines = "warn"  # warn, skip, error
    try:
        df = pd.read_csv(
            filepath,
            sep=sep,
            header=None,
            names=names,
            dtype=dtype,
            encoding=encoding,
            comment="#",
            on_bad_lines=on_bad_lines,
            # new (seems necessary, eg. for co2 funcs. otherwise removes trailing quotes)
            # not a good idea because then ocd_propertytext etc. keep \"...\"
            # but is necessary for funcs otherwiese values "" get removed
            quoting=quoting,
            # read None string as None string
            na_values=na_values,
            keep_default_na=False,
        )
    except (
        ValueError,
        FileNotFoundError,
    ) as e:
        return NotAvailable(e)
    # map changes dtype of column to object
    # for now ok because object = string but not good
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return Table(df, filepath, ofml_part_name)


def ofml_dtype_2_pandas_dtype(ofml_dtype):
    ofml_dtype = str.lower(ofml_dtype)
    if "string" in ofml_dtype:
        return "string"
    return ofml_dtype


@catch_file_exception
def read_pdata_inp_descr(file_name):
    result = OrderedDict()
    inside_comment = False
    with open(file_name, "r", encoding="cp1252") as file:
        for line in file:

            if line.isspace():
                continue

            row = line.split()

            if row[0] == "comment":
                inside_comment = True

            if len(row) >= 2 and f"{row[0]} {row[1]}" == "end comment":
                inside_comment = False

            if inside_comment:
                continue

            if row[0] == "table":
                table_name = row[2]
                result[table_name] = [[], [], ";"]
            elif row[0] == "field":
                field_name = row[2]
                datatype = row[3]
                datatype = ofml_dtype_2_pandas_dtype(datatype)
                delimiter = row[5] if len(row) >= 6 and row[4] == "delim" else ";"
                result[table_name][0].append(field_name)
                result[table_name][1].append(datatype)
                result[table_name][2] = delimiter
    return result
