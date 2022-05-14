from sqlalchemy import create_engine
import pandas as pd
import tempfile

import rrdata.rrdatad.index as index
from rrdata.utils.rqLogs import rq_util_log_info, rq_util_log_expection
from rrdata.utils.rqSetting import setting

database_uri = setting['POSTGRESQL']
rq_util_log_info(database_uri)
engine = create_engine(database_uri)
rq_util_log_info(engine)


def read_df_from_table(table_name, con=engine):
    df = pd.read_sql(table_name, con)
    rq_util_log_info(f"read sql from {table_name} \n{df}")
    return df


def read_large_df_from_table(sql_query, con=engine):
    """
    pandas has a built-in chunksize parameter 
    that you can use to control this sort of thing
    """
    dfs = []
    for chunk in pd.read_sql_query(sql_query, con, chunksize=5000):
        dfs.append(chunk)
    df = pd.concat(dfs)
    rq_util_log_info(df.tail())
    return df 


def read_sql_tmpfile(query, engine):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
           query=query, head="HEADER"
        )
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        rq_util_log_info(df)
        return df


if __name__ == '__main__':
    for t in ['swl_cons_L1','swl_cons_L2','swl_cons_L3']:
        read_df_from_table(t, engine)

    #sql_query = """SELECT index_code, index_name, level FROM swl_list  WHERE level='L3'  
    #            """
    #read_large_df_from_table(sql_query)
    #read_sql_tmpfile(sql_query, engine)
    #read_df_from_table('swl_cons_L1',engine)