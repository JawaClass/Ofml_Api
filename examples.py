

from repo.repository import Repository

print("examples...")

repo: Repository

repo = Repository(r"b:Testumgebung/EasternGraphics")

print(repo)

repo.read_profiles()
