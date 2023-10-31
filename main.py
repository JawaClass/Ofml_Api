from pathlib import Path

from repository import Repository

repo = Repository(Path(r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics'))

for name in repo.program_names():

    repo.load_program(name, keep_in_memory=True)

    program = repo[name]

    print(program)

    for ofml_part in program.featured_ofml_parts():
        program.load_ofml_part(ofml_part)
