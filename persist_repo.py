import time
from pathlib import Path
import asyncio
from repo.repository import Repository, Table
from db import update_table


test_env = r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics'
prod_env = r'\\w2_fs1\edv\knps-testumgebung\ofml_development\repository'
path = Path(test_env)


async def callback(t: asyncio.Task):
    table: Table = t.result()
    # print("# " * 40)
    # print(f"Callback for {t.get_name()}")
    # print(table.df.head().to_string())
    # print("# " * 40)
    # print("")

    await asyncio.to_thread(update_table, table, "quick3")


async def asyn_main():
    print("ASYNC")
    repo_ = Repository(path)
    repo_.read_profiles()

    program_names = "quick3".split()  # repo_.program_names()

    promises = [asyncio.to_thread(repo_.load_program, name, True) for name in program_names]

    await asyncio.gather(*promises)

    promises = [asyncio.to_thread(repo_[name].load_all) for name in program_names]
    await asyncio.gather(*promises)

    tasks = []

    for program in repo_.programs():

        print("make tasks for", program.name)

        if program.is_ocd_available():
            for name in program.ocd.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(program.ocd.read_table, name),
                        name=f"Task {len(tasks)} {program.name} ocd {name}"
                    )
                )

        if program.is_oam_available():
            for name in program.oam.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(program.oam.read_table, name),
                        name=f"Task {len(tasks)} {program.name} oam {name}"
                    )
                )

        if program.is_go_available():
            for name in program.go.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(program.go.read_table, name),
                        name=f"Task {len(tasks)} {program.name} go {name}"
                    )
                )

        if program.is_oap_available():
            for name in program.oap.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(program.oap.read_table, name),
                        name=f"Task {len(tasks)} {program.name} oap {name}"
                    )
                )

        if program.is_oas_available():
            for name in program.oas.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(program.oas.read_table, name),
                        name=f"Task {len(tasks)} {program.name} oas {name}"
                    )
                )

    for i, task in enumerate(tasks):

        task.add_done_callback(lambda t: asyncio.create_task(callback(t)))

    print("await tasks", len(tasks))
    await asyncio.gather(*tasks)


start = time.time()
asyncio.run(asyn_main())
print("t", time.time() - start)
