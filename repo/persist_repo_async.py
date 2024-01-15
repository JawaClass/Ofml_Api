from datetime import datetime
from pathlib import Path
import asyncio
import time
from loguru import logger
from typing import Coroutine, Any, Generator
from .repository_async import RepositoryAsync, ProgramAsync, Table
from .db_async import AsyncDatabaseInterface

TEST_ENV = r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics'
PROD_ENV = r'\\w2_fs1\edv\knps-testumgebung\ofml_development\repository'

ProgramAsyncTask = Coroutine[Any, Any, ProgramAsync]


async def main(plaintext_path: str):
    """
    reads all tables from repository @ plaintext_path asynchronously
    and writes all tables to database asynchronously

    """
    logger.debug(f"START PERSIST DB plaintext_path={plaintext_path}")

    repo = RepositoryAsync(Path(plaintext_path))

    # establish db connection and read profiles
    db: AsyncDatabaseInterface
    _, db = await asyncio.gather(
        repo.read_profiles(),
        AsyncDatabaseInterface.create(asyncio.get_event_loop())
    )

    # list that stores tasks to persist tables
    persist_tasks: list[asyncio.Task] = []

    load_program_tasks: list[ProgramAsyncTask] = []
    for name in ["quick3"]:### repo.program_names():
        logger.debug(f"load program {name}")
        load_program_tasks.append(repo.load_program(
            name,
            keep_in_memory=True
            ))

    def chunked(lst: list[ProgramAsyncTask], n: int) -> Generator:
        """
        return subsets of list as generator
        """
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # progress only subset at once
    for chunk in chunked(load_program_tasks, 5):

        chunked_programs = await asyncio.gather(*chunk)

        for p in chunked_programs:
            logger.debug(f"persist all {len(p.all_tables)} tables of program {p.name}")
            table: Table
            for table in p.all_tables:
                if not table.df.empty:
                    task: asyncio.Task = asyncio.create_task(
                        db.persist_table(table, program_name=p.name),
                        name=f"Persist {table.name} of {p.name}"
                    )
                    persist_tasks.append(task)

        del chunked_programs

    logger.debug("Persist all table ...")
    await asyncio.gather(*persist_tasks)
    await db.update_misc(path=repo.root)


def run_prod_env():
    run_with_path(PROD_ENV)


def run_test_env():
    run_with_path(TEST_ENV)


def run_with_path(path: str):
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    logger.debug(f"run_prod_env starting now at {timestamp}")
    start = time.time()
    asyncio.run(main(path))
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    logger.debug(f"run_prod_env finished at {timestamp} took {round(time.time() - start, 2)}s")


if __name__ == "__main__":
    print("works")
    run_test_env()
