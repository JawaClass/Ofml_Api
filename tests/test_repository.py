from pathlib import Path

from repo.repository import Repository


def test_load_repo():
    Repository(root=Path(r"b:\Testumgebung\EasternGraphics"))

