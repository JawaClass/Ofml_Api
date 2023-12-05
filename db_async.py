import asyncio
import aiomysql
from aiomysql.sa import create_engine

db_config = {
    "host": 'pdf2obs01.kn.local',
    "port": 3306,
    "user": 'root',
    "password": '',
    "db": 'ofml',
}


def create_sqlalchemy_engine(loop):
    return create_engine(**db_config, loop=loop)


class AsyncDatabaseInterface:

    def __init__(self, pool):
        self.pool: aiomysql.pool.Pool = pool

        print("AsyncDatabaseInterface::__init__", self.pool)

    @staticmethod
    async def create(event_loop: asyncio.AbstractEventLoop):
        pool = await aiomysql.create_pool(**db_config,
                                          loop=event_loop)

        return AsyncDatabaseInterface(pool)

    async def update(self, statement: str):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(statement)
                await conn.commit()

    async def select(self, statement: str):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(statement)
                rows = await cur.fetchall()
                print("Result__________________:")
                for _ in rows:
                    print(_)


async def main():
    loop = asyncio.get_event_loop()
    db = await AsyncDatabaseInterface.create(loop)
    await db.select("SELECT * FROM timestamp;")

    await db.update("INSERT INTO timestamp VALUES ('xxxx', 'yyyyy')")

