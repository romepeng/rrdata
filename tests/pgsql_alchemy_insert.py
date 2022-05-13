from pyparsing import conditionAsParseAction
from sqlalchemy import insert
from rrdata.utils.rqSetting import setting
from rrdata.utils.rqLogs import rq_util_log_info
import psycopg2
from sqlalchemy import create_engine

passwd_postgres = setting["PGSQL_PASSWORD"]
host_ip = setting["IP_DATABASE_ALIYUN"]
db_name = setting["DATABASE"]
user_table = "user" 


engine = create_engine(f"postgresql+psycopg2://postgres:{passwd_postgres}@{host_ip}/{db_name}", future=True)

rq_util_log_info(engine)

"""
stmt = '''
insert(user_table).values(name='sz', fullname="shezhen")
'''
# 查看原生SQL語句字串
print(stmt)
# INSERT INTO user_account (name, fullname) VALUES (:name, :fullname)

with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()
"""
# 批量插入
with engine.connect() as conn:
    result = conn.execute(
        insert(user_table),
        [
            {"name": "sandy", "fullname": "Sandy Cheeks"},
            {"name": "patrick", "fullname": "Patrick Star"}
        ]
    )
    conn.commit()