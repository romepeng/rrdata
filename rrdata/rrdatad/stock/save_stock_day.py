import logging
import pstats
from symtable import Symbol
import pandas as pd
import logging

import tushare as ts
from rrdata.rrdatad.stock.tusharepro import pro
from rrdata.rrdatad.stock.fetch_stock_list import fetch_stock_code_em
from rrdata.utils.rqCode import rq_util_str_tounitecode
from rrdata.utils.rqLogs import rq_util_log_debug, rq_util_log_info, rq_util_log_expection
from rrdata.rrdatad.stock import stock_zh_a_hist_em, stock_zh_a_spot_em
from rrdata.common import save_df_to_pgsql, read_df_from_table,engine
from rrdata.rrdatac.rrdataD_read_api import RrdataD

startDate = "20200101"
def get_stock_daily_one(symbol="600519",period="daily",start_date=startDate, end_date="20321231", adjust="hfq"):
    """ 
    hist daily from eastmoney
    adjust: : "hfq"
    #HowTO get stock adhust_factor
    """
    df = stock_zh_a_hist_em(symbol,period,start_date,end_date,adjust)
    df['ts_code'] = rq_util_str_tounitecode(symbol)
    df = df[['ts_code','trade_date','open','high','low','close','chg_pct','vol','amount']]
    return df


def get_stock_daily_adjfactor_one(symbol="600519",period="daily",start_date=startDate, end_date="20321231", adj="hfq", adjfactor=True):
    """  from tushare pro ts.pro_bar(adj="hfq', adjfactor=True)"""
    """"
    df_hfq = get_stock_daily_one(symbol=symbol, adjust='hfq')
    logging.info(f"\n {df_hfq}")
    df_bfq = get_stock_daily_one(symbol=symbol,adjust="")
    logging.info(f"\n {df_bfq}")
    df_adj = df_bfq.copy()
    df_adj['adjfactor'] = df_hfq['close'] / df_bfq['close']
    logging.info(f"\n {df_adj}")
    """
    ts_code = rq_util_str_tounitecode(symbol)
    df_hfq_adj_one = ts.pro_bar(ts_code=ts_code, start_date=startDate, adj=adj, adjfactor=adjfactor)
    #print(df_hfq_adj_one)
    df_adj_one = df_hfq_adj_one[['trade_date','ts_code','adj_factor']]
    print(df_adj_one)
    return df_adj_one


def save_stock_daily(stock_list=None,db_name='stock_day'):
    """TODO  use asyncio -- postgres+asyncpg://host/dbname --engine
        5000 class to 20 class 250 unit
    """
    lists = read_df_from_table('stock_list').symbol.values if None else stock_list
    rq_util_log_debug(lists)
    for symbol in lists:
        try:
            data = get_stock_daily_one(symbol)
            save_df_to_pgsql(data,db_name,if_exists='append')
        except Exception as e:
            rq_util_log_expection(e)


def save_adjfactor_to_pgsql(stock_list=None,db_name="adj_factor"):
    lists = fetch_stock_code_em()
    rq_util_log_debug(len(lists))
    for symbol in lists:
        try:
            data = get_stock_daily_adjfactor_one(symbol)
            save_df_to_pgsql(data,db_name,if_exists='append')
        except Exception as e:
            rq_util_log_expection(e)


def save_stock_daily_adj(stock_list=None,table_name='stock_day_adj'): #TODO
    """TODO  use asyncio -- postgres+asyncpg://host/dbname --engine
        5000 class to 20 class 250 unit
    """
    #lists = read_df_from_table('stock_list').symbol.values if None else stock_list
    lists = fetch_stock_code_em()
    rq_util_log_debug(len(lists))
  
    for symbol in lists[:1]:
        try:
            ts_code = rq_util_str_tounitecode(symbol)
            #data1 = get_stock_daily_one(symbol)
            data1 = ts.pro_bar(ts_code, start_date=startDate)
            data1.sort_values(by='trade_date', ascending=True, inplace=True)            
            data2 = RrdataD('adj_factor').read(instruments=rq_util_str_tounitecode(symbol))
            data2.sort_values(by='trade_date', ascending=False, inplace=True)
            data = pd.merge(data1, data2)
            rq_util_log_info(data)
            save_df_to_pgsql(data,table_name,engine(db_name="rrdata"),if_exists='append')
        except Exception as e:
            rq_util_log_expection(e)


if  __name__ == "__main__":
    #list1 = ['600519','000792','835185']
    #save_stock_daily(list1, 'stock_day_test')
   
    #read_df_from_table('stock_day_test')

    #df_bfq = get_stock_daily_one('600519')
    #print(df_bfq)

    #get_stock_daily_adjfactor_one('600519')
    #print(fetch_stock_code_em(ts_code=True))

    #save_adjfactor_to_pgsql()

    #read_df_from_table('adj_factor')
    save_stock_daily_adj()
    #print(RrdataD('stock_day_adj').read(start_date="2022-05-17"))
    #print(RrdataD('stock_day_adj').read(instruments=["000792.SZ",'600519.SH']))


