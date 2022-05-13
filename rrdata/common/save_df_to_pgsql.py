from tkinter import EXCEPTION
from sqlalchemy import create_engine
import pandas as pd

import rrdata.rrdatad.index as index
from rrdata.utils.rqLogs import rq_util_log_info, rq_util_log_expection
from rrdata.utils.rqSetting import setting


table_name = "swl_list"
database_uri = setting['POSTGRESQL']
rq_util_log_info(database_uri)

engine = create_engine(database_uri)
rq_util_log_info(engine)


def save_df_to_pgsql(data, table_name, con, if_exists='replace', index=False):
    rq_util_log_info(data)
    try:
        data.to_sql('swl_list',engine,if_exists='replace',index=False)
        rq_util_log_info(f"save data to {table_name}")
    except EXCEPTION as e:
        rq_util_log_expection(e)


if  __name__ == '__main__':
    data = index.sw_index_class_all()
    save_df_to_pgsql(data,'swl_list', engine)
    data2 = index.sw_index_cons()
    save_df_to_pgsql(data2, 'swl_cons', engine)



