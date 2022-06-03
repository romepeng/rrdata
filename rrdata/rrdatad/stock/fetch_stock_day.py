from cmath import nan
import time
from turtle import Turtle
import pandas as pd
from typing import Optional, List,Dict, Tuple

import tushare as ts
from rrdata.rrdatad.stock.tusharepro import pro
from rrdata.utils.rqDate import  rq_util_date_int2str
from rrdata.utils.rqDate_trade import rq_util_get_last_tradedate, rq_util_get_last_day,rq_util_get_pre_trade_date,rq_util_get_trade_range
from rrdata.rrdatad.stock.fetch_stock_list import fetch_stock_code_em
from rrdata.utils.rqCode import rq_util_str_tounitecode
from rrdata.utils.rqLogs import rq_util_log_debug, rq_util_log_info, rq_util_log_expection
from rrdata.utils.rqDate import rq_util_date_str2int
from rrdata.common import save_df_to_pgsql, read_df_from_table,read_data_from_table, engine
from rrdata.common.get_update_tradedate import get_wanted_record_tradedate, get_wanted_record_tradedate_check_tscode

from rrdata.common.read_data_from_table import read_one_row_from_table, read_one_row_from_table_check_tscode
from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.utils.rqParameter import startDate


#startDate = "2021-04-28"
last_tradedate = rq_util_get_last_tradedate().replace("-","")
#startDate = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(),255).replace("-","")
print(last_tradedate, startDate)


def fetch_stock_list_tspro()-> pd.DataFrame:
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    #stock_list_D= pro.stock_basic(exchange_id='', is_hs='',list_status='D' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    #stock_list=pd.concat([stock_list_l,stock_list_D],axis=0)
    stock_list=pd.concat([stock_list_l,stock_list_P],axis=0)
    #stock_list = stock_list_l
    stock_list['code']=stock_list['symbol']
    #rq_util_log_info(stock_list)
    return stock_list


def fetch_adj_factor_tspro(trade_date=last_tradedate): #very importtant
    return pro.query('adj_factor',trade_date=last_tradedate)
    

def fetch_stock_daily_hfq_one_tspro(ts_code='600519.SH',asset="E", adj='hfq', start_date=startDate, end_date=None,adjfactor=True):
    return ts.pro_bar(ts_code=ts_code, asset=asset,adj=adj, start_date=start_date, end_date=end_date, adjfactor=adjfactor) 

def fetch_stock_daily_adjfactor_one(symbol="600519", period="daily",start_date=startDate, end_date="20321231", adj="hfq", adjfactor=True):
    """  from tushare pro ts.pro_bar(adj="hfq', adjfactor=True)"""
    ts_code = rq_util_str_tounitecode(symbol)
    df_hfq_adj_one = ts.pro_bar(ts_code=ts_code, start_date=startDate, adj=adj, adjfactor=adjfactor)
    return df_hfq_adj_one[['trade_date','ts_code','adj_factor']]
    

def fetch_stock_day_bfq_from_tspro(ts_code=None,trade_date=rq_util_get_last_tradedate().replace("-",""),start_date=None,end_date=None):   
    trade_date = trade_date.replace('-', '') #兼容设置以防日期格式为2001-10-20格式
    for _ in range(3):
        try:
            if trade_date:
                df = pro.daily(ts_code=ts_code, trade_date=trade_date)
            else:
                df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except:
            time.sleep(1)
        else:
            return df
        

def fetch_stock_daily_hfq_fillna_tspro(ts_code='600519.SH',asset="E", adj='hfq', 
                                       start_date=startDate, end_date=None,adjfactor=True):
    
    trade_list = rq_util_get_trade_range(start_date,rq_util_get_last_tradedate())
    trade_list = list(map(lambda x: x.replace("-",""), trade_list))
    """
    cal = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    start_date = start_date.replace("-","")
    end_date = last_tradedate 
    trade_cal = cal[cal["is_open"] == 1] # and  
    trade_cal = trade_cal[(trade_cal['cal_date'] >= start_date)  & (trade_cal['cal_date'] <= last_tradedate)]
    #trade_cal = trade_cal['cal_date']
    print(start_date)
    print(trade_cal)
    """
    
    df_list_date = fetch_stock_list_tspro()[['ts_code','list_date']]
    list_date = df_list_date[df_list_date.ts_code == ts_code]['list_date'].values[0]
    #print(list_date)
    
    #df_trade_date = trade_cal[trade_cal['cal_date'] >= list_date][['cal_date']]
    #df_trade_date.rename(columns={'cal_trade':'trade_date'},inplace=True)
    #print(df_trade_date)
    trade_date_code = [x  for x in trade_list  if x >= list_date]
    #print(trade_date_code)

    
    df_one = ts.pro_bar(ts_code=ts_code, asset=asset,adj=adj, start_date=start_date, end_date=end_date, adjfactor=adjfactor)
    df_one = df_one.drop(columns='change').sort_values(by='trade_date', ascending=True)
    
    print(df_one)
    
    fill_row = [x for x in trade_date_code if x not in df_one.trade_date.values]
    print(fill_row)
    
    df_fill = pd.DataFrame(data=trade_date_code, columns=['trade_date'])
    df_fill['ts_code'] = ts_code
    #for row in  [ "pct_chg", "vol", "amount"]:
    #    df_fill[row] = 0
    print(df_fill)
        
    df_one= pd.merge(df_fill, df_one,how='left')
    df_one.fillna({'pct_chg':0,'vol':0,'amount':0},inplace=True)
    df_one.fillna(method='ffill',inplace=True)
    print(df_one)
    
    return df_one
    

     

def fetch_stock_daily_bfq_adj_fillna_tspro(ts_code=None,start_date=startDate, end_date=None,adjfactor=True):
    
    trade_list = rq_util_get_trade_range(start_date,rq_util_get_last_tradedate())
    trade_list = list(map(lambda x: x.replace("-",""), trade_list))
    
    df_list_date = fetch_stock_list_tspro()[['ts_code','list_date']]
    list_date = df_list_date[df_list_date.ts_code == ts_code]['list_date'].values[0]
    #print(list_date)
    
    trade_date_code = [x  for x in trade_list  if x >= list_date]
    #print(trade_date_code)

    
    
    fill_row = [x for x in trade_date_code if x not in df_one.trade_date.values]
    print(fill_row)
    
    df_fill = pd.DataFrame(data=trade_date_code, columns=['trade_date'])
    df_fill['ts_code'] = ts_code
    #for row in  [ "pct_chg", "vol", "amount"]:
    #    df_fill[row] = 0
    print(df_fill)
        
    
    

if  __name__ == "__main__":
    #print(fetch_stock_daily_hfq_one_tspro('600060.SH'))
    #print(fetch_stock_daily_adjfactor_one())
    #print(fetch_stock_day_bfq_from_tspro(trade_date="20220518"))
           
    #print(read_df_from_table("stock_day"))
    t1 = time.perf_counter()
    #print(fetch_stock_list_tspro())
    #print(fetch_stock_daily_hfq_one_tspro('600519.SH'))
    fetch_stock_daily_hfq_fillna_tspro(ts_code='002770.SZ')
    t2 = time.perf_counter()
    t = t2 - t1
    print(f"times:  --- {t}")
    
       
    pass


