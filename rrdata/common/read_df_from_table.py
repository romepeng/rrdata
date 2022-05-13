from sqlalchemy import create_engine
import pandas as pd

import rrdata.rrdatad.index as index
from rrdata.utils.rqLogs import rq_util_log_info, rq_util_log_expection
from rrdata.utils.rqSetting import setting

database_uri = setting['POSTGRESQL']
rq_util_log_info(database_uri)
engine = create_engine(database_uri)
rq_util_log_info(engine)


def read_df_from_table(table_name, engine=engine):
    df = pd.read_sql(table_name, engine)
    rq_util_log_info(f"read sql from {table_name} \n{df}")
    return df

if __name__ == '__main__':
    read_df_from_table('swl_list', engine)