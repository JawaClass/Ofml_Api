from ofml_api.repository import read_table
from config import ROOT_PATH


def read_with_none_in_column():
    """
    by default pandas replaces "None" strings with NA
    exclude "None" from this behaviour
    """

    # field   1	id			vstring		delim ; trim hidx link
    # field   2	property		vstring		delim ; trim
    # field   3	condition		vstring		delim ; trim
    # field   4	state_restr		vstring		delim ; trim
    filepath = ROOT_PATH / "tests" / "resources" / "column_with_none.csv"

    table = read_table(
        filepath,
        names=["id", "property", "condition", "state_restr"],
        dtype={
            "id": "str",
            "property": "str",
            "condition": "str",
            "state_restr": "str",
        },
        encoding="utf8",
        ofml_part_name="oap",
    )

    assert (
        table.df["state_restr"].values.tolist()
        == "VisibleEditable None None None None".split()
    ), "doesnt read string None as string None"


if __name__ == "__main__":
    read_with_none_in_column()
