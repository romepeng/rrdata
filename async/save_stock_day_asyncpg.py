import time
from tracemalloc import start
from unittest import result
import pandas as pd
import asyncio
import asyncpg

import tushare as ts
from rrdata.rrdatad.stock.tusharepro import pro
from rrdata.utils.rqDate import  rq_util_date_int2str
from rrdata.utils.rqDate_trade import rq_util_get_last_tradedate, rq_util_get_last_day,rq_util_get_pre_trade_date,rq_util_get_trade_range
from rrdata.rrdatad.stock.fetch_stock_list import fetch_stock_code_em
from rrdata.utils.rqCode import rq_util_str_tounitecode
from rrdata.utils.rqLogs import rq_util_log_debug, rq_util_log_info, rq_util_log_expection

from rrdata.common import save_df_to_pgsql, read_df_from_table,read_data_from_table, engine
from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.utils.rqSetting import setting

#startDate = "20210501"
last_tradedate = rq_util_get_last_tradedate().replace("-","")
startDate = rq_util_get_pre_trade_date(rq_util_get_last_tradedate(),255).replace("-","")
#print(last_tradedate, startDate)
uri = setting['ASYNCPG_RRDATA_URI']
print(uri)


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



def save_stock_day_hfq_to_pgsql(stock_list=None,start_date=startDate,table_name='stock_day_hfq'):
    """TODO  use asyncio -- postgres+asyncpg://host/dbname --engine
        5000 class to 20 class 250 unit
        two factor: ts_coe /trade_date
    """
    #lists = read_df_from_table('stock_list').symbol.values if None else stock_list
    lists = fetch_stock_list_tspro().ts_code.to_list()
    print(f"{len(lists)} \n {lists[:10]}")
    
    
    t1 = time.time()
    for ts_code in lists:
        try:
            print(ts_code)
            #ts_code = rq_util_str_tounitecode(symbol)
            #data1 = get_stock_daily_one(symbol)
            data1 = ts.pro_bar(ts_code=ts_code, start_date=start_date, adj='hfq', adjfactor=True)
            data1.sort_values(by='trade_date', ascending=True, inplace=True)            
            #data2 = RrdataD('adj_factor').read(instruments=rq_util_str_tounitecode(symbol))
            #data2.sort_values(by='trade_date', ascending=False, inplace=True)
            #data = pd.merge(data1, data2)
            print(data1)
            save_df_to_pgsql(data1,table_name,if_exists='append')
        except Exception as e:
            rq_util_log_expection(e)
      
    t2 = time.time()
    print(f"times: {t2 - t1}")


async def save_stock_day_hfq_to_asyncpg(ts_code=None, start_date=startDate, table_name="stock_day_hfq_async"):
    
    lists = fetch_stock_list_tspro().ts_code.to_list()
    print(f"{len(lists)} \n {lists[:10]}")
        
    async def fetch_and_save_stock_hfq_to_pgsql_one(conn=engine(),ts_code=ts_code):
        one = ts.pro_bar(ts_code=ts_code, start_date=start_date, adj='hfq', adjfactor=True)
        one.sort_values(by='trade_date', ascending=True, inplace=True)            
        print(one)
        save_df_to_pgsql(one, table_name, conn, if_exists='append')
        print('save ok!')
        
    
    async with asyncpg.create_pool(uri) as pool:
               
        # query_list =[] 
        count = len(lists[:55]) // 10   # lists slip to 10 group
        
        task_list = [lists[c*count: (c+1)*count]  for c in range(10+1)]
        print(task_list)
        
        tasks = []
        for t in task_list:
            conn = await pool.acquire()
            print(conn)
            print(fetch_and_save_stock_hfq_to_pgsql_one(conn,t))
            tasks.append(fetch_and_save_stock_hfq_to_pgsql_one(conn,t))
        results = await asyncio.gather(*tasks)
        return results
    
        
        

if  __name__ == "__main__":
    
    #save_stock_day_hfq_to_pgsql()
    #df = RrdataD('stock_day_hfq').read()
    #print(df)

  
    #df = RrdataD('stock_day_bfq').read(instruments=['000792.SZ'])
    #print(df)

    #print(read_df_from_table("stock_day"))
    
    start =time.perf_counter()
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(save_stock_day_hfq_to_asyncpg(ts_code=None, start_date=startDate, table_name="stock_day_hfq_async"))
    end = time.perf_counter()
    print(f"times : {end - start}")
    for res in results:
        for _ in result:
            print()

    
