import time
from pathlib import Path
import asyncio
import aiomysql
import pymysql.err

from repo.repository import Repository, Table, NotAvailable
from db_async import AsyncDatabaseInterface

test_env = r'\\w2_fs1\edv\knps-testumgebung\Testumgebung\EasternGraphics'
prod_env = r'\\w2_fs1\edv\knps-testumgebung\ofml_development\repository'
path = Path(prod_env)


async def persist_table(table: Table, db: AsyncDatabaseInterface, program_name):

    async with db.pool.acquire() as conn:
        cur: aiomysql.Cursor
        async with conn.cursor() as cur:
            try:
                await cur.execute(f"DELETE FROM {table.database_table_name} WHERE sql_db_program=%s;", (program_name, ))
                await conn.commit()
            except pymysql.err.ProgrammingError as e:
                print(f"ERR :: persist_table {table.name} _ {table.database_table_name} in {program_name}", e)
                return

            if not table.df.empty:
                table.df.fillna(value='', inplace=True)

                table.df["sql_db_program"] = program_name
                table.df["sql_db_timestamp_modified"] = table.timestamp_modified
                table.df["sql_db_timestamp_read"] = table.timestamp_read

                column_names = ", ".join([f"`{_}`" for _ in list(table.df.columns)])
                value_placeholders = ", ".join(["%s" for _ in table.df.columns])
                data = table.df.values.tolist()
                stmt = f"INSERT INTO {table.database_table_name} ({column_names}) VALUES ({value_placeholders});"

                await cur.executemany(stmt, data)
                await conn.commit()

    # print("Callback Done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", table.name)


async def asyn_main():
    # TODO: make so that n (param) programs can run concurrently

    def read_table_wrapper(read_table, table_name, program_name):
        def f():
            return {
                "table": read_table(table_name),
                "program": program_name
            }
        return f

    print("ASYNC")

    repo_ = Repository(path)
    repo_.read_profiles()

    program_names = repo_.program_names()#"quick3 talos".split() # repo_.program_names()#  # repo_.program_names()#   # repo_.program_names()

    promises = [asyncio.to_thread(repo_.load_program, name, True) for name in program_names]

    await asyncio.gather(*promises)

    promises = [asyncio.to_thread(repo_[name].load_all) for name in program_names]
    await asyncio.gather(*promises)

    tasks = []

    database_tasks = []

    for program in repo_.programs():

        print("make plaintext tasks for", program.name)

        if program.is_ocd_available():
            for name in program.ocd.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            read_table_wrapper(program.ocd.read_table, name, program.name)
                        ),
                        name=f"Task {len(tasks)} {program.name} ocd {name}"
                    )
                )

        if program.is_oam_available():
            for name in program.oam.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            read_table_wrapper(program.oam.read_table, name, program.name)
                            ),
                        name=f"Task {len(tasks)} {program.name} oam {name}"
                    )
                )

        if program.is_go_available():
            for name in program.go.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            read_table_wrapper(program.go.read_table, name, program.name)
                        ),
                        name=f"Task {len(tasks)} {program.name} go {name}"
                    )
                )

        if program.is_oap_available():
            for name in program.oap.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            read_table_wrapper(program.oap.read_table, name, program.name)),
                        name=f"Task {len(tasks)} {program.name} oap {name}"
                    )
                )

        if program.is_oas_available():
            for name in program.oas.filenames:
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            read_table_wrapper(program.oas.read_table, name, program.name)),
                        name=f"Task {len(tasks)} {program.name} oas {name}"
                    )
                )

    # read all plaintext
    results = await asyncio.gather(*tasks)

    db = await AsyncDatabaseInterface.create(asyncio.get_event_loop())

    for res in results:
        # print("result:::", type(res), res)
        table = res["table"]
        program = res["program"]

        if table is None or type(table) is NotAvailable:
            continue

        if table.df.empty:
            continue

        database_tasks.append(
            asyncio.create_task(
                persist_table(db=db, table=table, program_name=program)
            )
        )

    print("plaintext tasks done", database_tasks)

    await asyncio.gather(*database_tasks)

    # await asyncio.sleep(5)
    print("database tasks done", database_tasks)
    print("Done............................................", len(tasks), len(database_tasks))


start = time.time()
asyncio.run(asyn_main())
print("t", time.time() - start)
