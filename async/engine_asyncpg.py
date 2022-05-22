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
database_dns = f"postgresql://postgres:{passwd}@{host}:5432"
rq_util_log_info(database_dns)

    
async def engine_asyncpg(db_name='rrdata"'):
    """
           usage:
        conn = await asyncpg.connect()
        con = engine_asyncpg(db_name)
        asyncio.get_event_loop().run_until_complete(func())
    """
    return await asyncpg.connect(f"postgresql://{database_str}/{db_name}")

def engine_async(db_name="rrdata"): #TODO
    return create_async_engine(f"postgresql+asyncpg://{database_str}/{db_name}",
             #  connect_args={"server_settings": {"jit": "off"}},
             )


if  __name__ == "__main__":
   
    rq_util_log_info(engine_async())
    #rq_util_log_info(engine_asyncpg())
    
        