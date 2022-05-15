import pstats
from symtable import Symbol
import pandas as pd

from rrdata.utils.rqCode import rq_util_str_tounitecode
from rrdata.rrdatad.stock import stock_zh_a_hist, stock_zh_a_spot_em
from rrdata.common import save_df_to_pgsql, read_df_from_table, engine
 

startDate = "20220501"
def get_stock_a_daily_one(symbol="600519",period="daily",start_date=startDate, end_date="20321231", adjust="hfq"):
    """ 
    hist daily from eastmoney
    adjust: : "hfq"
    #HowTO get stock adhust_factor
    """
    df = stock_zh_a_hist(symbol,period,start_date,end_date,adjust)
    df['ts_code'] = rq_util_str_tounitecode(symbol)
    df = df[['ts_code','trade_date','open','high','low','close','chg_pct','vol','amount']]
    return df

def save_stock_a_daily_all():
    """TODO  use asyncio -- postgres+asyncpg://host/dbname --engine
        5000 class to 20 class 250 unit

    """
    stock_list = read_df_from_table('stock_list').symbol.values[:2]
    print(stock_list)
    for symbol in stock_list:
        try:
            data = get_stock_a_daily_one(symbol)
            save_df_to_pgsql(data,'stock_day',con=engine,if_exists='append')
        except:
            pass

    

if  __name__ == "__main__":
    data1 = get_stock_a_daily_one("873169")
    save_df_to_pgsql(data1,'data1')
    #read_df_from_table('data1')
    #save_stock_a_daily_all()
    
    #read_df_from_table('swl_list')


