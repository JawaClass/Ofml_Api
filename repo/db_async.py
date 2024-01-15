import asyncio
import aiomysql
from loguru import logger
from .repository import Table
from settings import db_config


class AsyncDatabaseInterface:

    def __init__(self, pool):
        self.pool: aiomysql.pool.Pool = pool

    @staticmethod
    async def create(event_loop: asyncio.AbstractEventLoop):
        pool = await aiomysql.create_pool(**db_config,
                                          loop=event_loop)

        return AsyncDatabaseInterface(pool)

    async def update_misc(self, **kwargs):
        import datetime
        timestamp = datetime.datetime.now()
        date = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
        path = kwargs["path"]
        await self.update(
                f"""
                        DELETE FROM timestamp;  
                """)

        await asyncio.gather(
            self.update(
                f"""
                        INSERT INTO timestamp (date, type) VALUES  (%s, "init_tables");
                """, (date, )),
            self.update(
                f"""
                        INSERT INTO timestamp (date, type) VALUES  (%s, "path");
                """, (path, ))
        )

    async def update(self, statement: str, args=None):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(statement, args)
                await conn.commit()

    async def persist_table(self, table: Table, program_name):

        async with self.pool.acquire() as conn:
            cur: aiomysql.Cursor
            async with conn.cursor() as cur:

                try:
                    await cur.execute(f"DELETE FROM {table.database_table_name} WHERE sql_db_program=%s;",
                                      (program_name,))
                    await conn.commit()
                except Exception as e:
                    logger.error(f"persist_table failed {table.name} _ {table.database_table_name} in {program_name} | {e}")
                    return
                else:
                    # logger.info(f"persist_table success {table.name} _ {table.database_table_name} in {program_name}")
                    pass

                table.df.fillna(value='', inplace=True)

                table.df["sql_db_program"] = program_name
                table.df["sql_db_timestamp_modified"] = table.timestamp_modified
                table.df["sql_db_timestamp_read"] = table.timestamp_read

                column_names = ", ".join([f"`{_}`" for _ in list(table.df.columns)])
                value_placeholders = ", ".join(["%s" for _ in table.df.columns])
                data = table.df.values.tolist()
                stmt = f"INSERT INTO {table.database_table_name} ({column_names}) VALUES ({value_placeholders});"

                await cur.executemany(stmt, data)
                return await conn.commit()
