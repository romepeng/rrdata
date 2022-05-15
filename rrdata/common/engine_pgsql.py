import psycopg2
import asyncio
import asyncpg
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from rrdata.utils.rqLogs import rq_util_log_info
from rrdata.utils.rqSetting import setting

host = setting["IP_DATABASE_ALIYUN"]
passwd = setting["PGSQL_PASSWORD"]
database_str = f"postgres:{passwd}@{host}:5432"
rq_util_log_info(database_str)


def engine(driver="psycopg2", db_name="rrdata"):
    database_uri_str = f"{database_str}/{db_name}"
    if not driver:
        database_uri = f"postgresql://{database_uri_str}"
        #rq_util_log_info(database_uri)
        cnx = create_engine(database_uri)
    if driver:
        cnx = create_engine(f"postgresql+{driver}://{database_str}/{db_name}")
    return cnx


async def engine_async(db_name='rrdata'):
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:{passwd}@{host}:5432/{db_name}", echo=True,
    )

    async with engine.begin() as conn:
        await conn.run_sync(meta.drop_all)
        await conn.run_sync(meta.create_all)

        await conn.execute(
            t1.insert(), [{"name": "some name 1"}, {"name": "some name 2"}]
        )

    async with engine.connect() as conn:

        # select a Result, which will be delivered with buffered
        # results
        result = await conn.execute(select(t1).where(t1.c.name == "some name 1"))

        print(result.fetchall())

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()

asyncio.run(async_main())

def engine_async(db_name="rrdata"): #TODO
    return create_async_engine(f"postgresql+asyncpg://{database_str}/{db_name}",
               connect_args={"server_settings": {"jit": "off"}},
             )

if  __name__ == "__main__":
    rq_util_log_info(engine(db_name='rrshare'))
    rq_util_log_info(engine_async())
    
        