# coding:utf-8
import asyncio
import asyncpg
import psycopg2

host = "39.106.68.163"
db_name = "rrdata"

uri = f'postgresql://{host}:5432/{db_name}'


def rq_util_sql_postgres_setting(uri=uri):
    """
    explanation:
        根据给定的uri返回一个postgresClient实例，采用@几何建议以使用加密
    params:
        * uri ->:
            meaning: postgresql接uri
            type: str
            optional: [null]
    return:
        postgresClient
    demonstrate:
        Not described
    output:
        Not described
    """

    # 采用@几何的建议,使用uri代替ip,port的连接方式
    # 这样可以对postgresql进行加密:
    # uri=postgresql://user:passwor@ip:port
    client = psycopg2.connect(uri)
    print(client)
    return client


# async

def rq_util_sql_async_postgres_setting(uri=uri):
    """
    explanation:
        根据给定的uri返回一个异步AsyncIOMotorClient实例
    params:
        * uri ->:
            meaning: postgresql连接uri
            type: str
            optional: [null]
    return:
        AsyncIOMotorClient
    demonstrate:
        Not described
    output:
        Not described
    """
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # async def client():
    return loop
    #.run_until_complete(uri)
    # yield  client()


##ASCENDING = psycopg2.ASCENDING
##DESCENDING = psycopg2.DESCENDING
#rq_util_sql_postgres_sort_ASCENDING = psycopg2.ASCENDING
#rq_util_sql_postgres_sort_DESCENDING = psycopg2.DESCENDING

if __name__ == '__main__':
    # test async_postgres
    client = rq_util_sql_async_postgres_setting()
    #.rrdata.stock_day
    #print(client)

