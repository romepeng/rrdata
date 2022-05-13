from sqlalchemy import create_engine
import pandas as pd

import rrdata.rrdatad.index as index
from rrdata.utils.rqLogs import rq_util_log_info, rq_util_log_expection
from rrdata.utils.rqSetting import setting
#from tomlkit import table


table_name = "swl_list"
database_uri = setting['POSTGRESQL']
rq_util_log_info(database_uri)

engine = create_engine(database_uri)
rq_util_log_info(engine)

def save_df_to_pgsql(df, table_name, con, if_exists='replce',index=False):
    """dataframe(df) save to table_name, if exists, replace 
    """    
    df = index.sw_index_class_all()
    #rq_util_log_info(df)
    df.to_sql('swl_list',engine,if_exists='replace',index=False)
    rq_util_log_info(f"saved dataframe to {table_name}")


