from pathlib import Path
import time
from ofml_api.repository import Repository, Program, OFMLPart, NotAvailable, Table
from pprint import pprint
import pandas as pd

REPO_ROOT = Path(r"b:Testumgebung/EasternGraphics")

def test_load_repo():
    repo = Repository(root=REPO_ROOT, manufacturer="kn")
    assert not repo.profiles
    repo.read_profiles()
    assert repo.profiles
    assert len(repo.program_names())
    assert not len(repo.programs())
    repo.load_program("talos")
    assert len(repo.programs()) == 1
    talos = repo["talos"]
    assert isinstance(talos, Program)    
    assert talos.contains_ofml_part("ocd")
    ocd = talos.load_ofml_part("ocd")
    assert isinstance(ocd, OFMLPart)
    

def test_program_not_available():
    repo = Repository(root=REPO_ROOT, manufacturer="kn")
    repo.read_profiles()
    program = repo.load_program("NOT_AVAILABLE_PROGRAM_NAME")
    assert isinstance(program, NotAvailable)


def test_table_not_available():
    repo = Repository(root=REPO_ROOT, manufacturer="kn")
    repo.read_profiles()
    program = repo.load_program("talos")
    program.load_ofml_part("ocd")
    table = program.ocd.read_table("NOT_AVAILABLE_TABLE_NAME")
    assert isinstance(table, NotAvailable)
    

def test_load_all():
    repo = Repository(root=REPO_ROOT, manufacturer="kn")
    repo.read_profiles()
    repo.load_program("talos")
    talos = repo["talos"]
    assert talos.featured_ofml_parts() == ['ocd', 'oam', 'go', 'oap', 'odb']
    talos.load_all_ofml_parts()
    assert isinstance(talos.ocd, OFMLPart)
    assert isinstance(talos.oam, OFMLPart)
    assert isinstance(talos.go, OFMLPart)
    assert isinstance(talos.oap, OFMLPart)
    assert isinstance(talos.odb, OFMLPart)
    talos.ocd.read_table("ocd_article.csv")
    ocd_article = talos.ocd.table("ocd_article.csv")
    assert isinstance(ocd_article, Table)
    table = talos.ocd.read_table("ocd_article222.csv")
    assert isinstance(table, NotAvailable)
    assert len(talos.ocd.tables) == 1 
    assert isinstance(talos.ocd.table("ocd_article").df, pd.DataFrame)
    
   