import time
from pathlib import Path
import asyncio
from repo.repository import Repository, Program

test_env = r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics'
prod_env = r'\\w2_fs1\edv\knps-testumgebung\ofml_development\repository'
path = Path(test_env)


async def callback(result, task_name):
    print(f"Callback for {task_name}: {result}")


async def asyn_main():
    print("ASYNC")
    repo_ = Repository(path)
    repo_.read_profiles()

    promises = [asyncio.to_thread(repo_.load_program, name, True) for name in repo_.program_names()]

    await asyncio.gather(*promises)

    repo_["table"].load_all()

    tasks = []

    task_objects = [asyncio.create_task(task) for task in tasks]

    for name in repo_["table"].ocd.filenames:
        tasks.append(
            asyncio.create_task(
                asyncio.to_thread(repo_["table"].ocd.read_table, name)
            )
        )

    for name in repo_["table"].oam.filenames:
        tasks.append(
            asyncio.create_task(
                asyncio.to_thread(repo_["table"].oam.read_table, name)
            )
        )

    for name in repo_["table"].go.filenames:
        tasks.append(
            asyncio.create_task(
                asyncio.to_thread(repo_["table"].go.read_table, name)
            )
        )

    for name in repo_["table"].oap.filenames:
        tasks.append(
            asyncio.create_task(
                asyncio.to_thread(repo_["table"].oap.read_table, name)
            )
        )

    for i, task in enumerate(tasks):
        task.my_callback_name = f"Task {i + 1}"
        task.add_done_callback(lambda t: asyncio.create_task(callback(t.result(), t.my_callback_name)))

    await asyncio.gather(*tasks)

    # print(repo_["table"].ocd.table("ocd_article").df.to_string())

start = time.time()
asyncio.run(asyn_main())
print("t", time.time() - start)