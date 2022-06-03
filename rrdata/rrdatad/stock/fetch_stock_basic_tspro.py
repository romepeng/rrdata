from typing import List
import pandas as pd

import tushare as ts
from rrdata.common.tusharepro import pro
from rrdata.utils.rqDate_trade import rq_util_get_last_tradedate
from rrdata.utils.rqCode import rq_util_str_tounitecode


def fetch_delist_stock(trade_date=None) -> List:
    df_P = pro.stock_basic(exchange='',list_status='P',fields='ts_code,symbol,name,list_status,delist_date,list_date')
    df_D = pro.stock_basic(exchange='',list_status='D',fields='ts_code,symbol,name,list_status,delist_date,list_date')
    df_DP = pd.concat([df_P, df_D], axis=0)
    if not trade_date:
        trade_date = rq_util_get_last_tradedate().replace('-','')
    df_DD = df_DP[df_DP['delist_date'] <= trade_date]
    return  list(df_DD['symbol'].values)


def fetch_stock_list_all()-> pd.DataFrame:    
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_D= pro.stock_basic(exchange_id='', is_hs='',list_status='D' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    stock_list=pd.concat([stock_list_l,stock_list_D],axis=0)
    stock_list=pd.concat([stock_list,stock_list_P],axis=0)
    return stock_list


def fetch_stock_list(df=True,ts_code=True):    
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    stock_list=pd.concat([stock_list_l,stock_list_P],axis=0)
    if df:
        return stock_list
    if ts_code:
        return list(stock_list['ts_code'].values)
    else:
        return list(stock_list['symbol'].values)


def fetch_stock_list_adj(trade_date=None)-> pd.DataFrame:
    if not trade_date:
        trade_date = rq_util_get_last_tradedate().replace('-','')
    return pro.adj_factor(trade_date=trade_date)
  

def fetch_pro_bar(ts_code="600519.SH",asset="E", adj='hfq', start_date='20180101', end_date='20181011'):
    return ts.pro_bar(ts_code=ts_code, asset=asset,adj=adj, start_date=start_date, end_date=end_date) 


if __name__ == '__main__':
    print(fetch_delist_stock())
    print(fetch_stock_list())
    print(fetch_stock_list_adj())
   

