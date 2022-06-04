# coding: utf-8
"""multi theard use threading.Thread and queue.Queue;
    pipline -- queue(q=queue.Queue);
    first q.put(id)  then queue.get()=id ;
    NUM_THREADS  decide by api speed limit(200/mins) and id totoals : lists // speed_per_mins
    every thead group num -- lists // NUM_TREADS
    开 NUM_TREADS 条线程， 每个线程有 NUM 项任务。
    API 速度限制： 60秒 - 200条请求的时间  --> time.sleeep(x) (if x > 0).
    TODO: hot to use many tspro_token auto change. get count of api bar_pro   
"""

import queue
import threading
import time
import pandas as pd
import multitasking
import tqdm
from typing import Dict, List

from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.rrdatad.rrdataD_save_api import RrdataDSave
from rrdata.rrdatad.stock.fetch_stock_day import fetch_stock_list_tspro
from rrdata.utils.rqSetting import setting
from rrdata.utils.rqParameter import startDate,PERIODS
from rrdata.utils.rqDate_trade import rq_util_get_trade_range, rq_util_get_last_tradedate

NList = PERIODS().PERIODS
N1List = list(map(lambda x: x-1,NList))

q = queue.Queue()
workers = []
NUM_THREADS = 50
N = 501

df = RrdataD('stock_list').read()
df.sort_values(by="ts_code", inplace=True)
print(df.head())
stock_list = df.ts_code.values
#stock_dict = df['ts_code'].to_dict()
#print(stock_list[1])
NUM = len(stock_list) // NUM_THREADS
print(NUM)


def stock_day_ma_rt_oh_save_tosql(queue, table_name='stock_day_ma_rt_oh_t1'):
    id = queue.get()
    print(f"get id : -- {id}")
    try:
        data = get_stock_day_ma_rt_oh(stock_list[id])
        RrdataDSave(table_name,if_exists='append').save(data)
    except Exception as E:
        print(E)


def main():
    for i in range(NUM_THREADS + 1):
        for j in range(NUM):
            worker = threading.Thread(target=stock_day_ma_rt_oh_save_tosql, args=(q,))
            worker.start()
            workers.append(worker)
            if (i * NUM + j) >= len(stock_list):
                break
            else:
                continue
            
    for i in range(NUM_THREADS + 1):
               
        for j in range(NUM):
            id = i * NUM + j
            #print(id)
            q.put(id)
            if (id + 2) >= len(stock_list):
                break
            else:
                continue
                
    for w in workers:
        w.join()
        

def save_stock_day_ma_rt_oh_to_pgsql():
    """ """  
    dfs: List[pd.DataFrame] = []
    for stock_code in stock_list:
        try:
            s = get_stock_day_ma_rt_oh(stock_code)
            dfs.append(s)
            print(dfs)
        except Exception as e:
            print(e)
    try:
        df = pd.DataFrame(dfs)
        RrdataDSave('stock_day_ma_rt_oh',if_exists='replace').save(df)
    except Exception as e:
        print(e)
   
         

def get_stock_day_ma_rt_pre(ts_code=None,N=250)-> pd.DataFrame:
    df = RrdataD('stock_day_bfq_adj').read(instruments=ts_code, count=N)
    df.drop_duplicates(keep='last',inplace=True)
    df = df.sort_values(by="trade_date", ascending=True)
    #print(df)
    i = 0
    while df.loc[0, 'vol'] == 0 :
        i += 120
        #print(i)
        df = RrdataD('stock_day_bfq_adj').read(instruments=ts_code, count=N+i)
        df = df.sort_values(by="trade_date", ascending=True)
    #print(df)
    df.fillna(method='ffill',inplace=True)
    # save result
    df = df.iloc[-(N+1): ]
    #print(df)
    
    vol_mean =  (df['vol']*df['adj_factor']).rolling(39).mean()
    vol_chg = 100*((df['vol']*df['adj_factor']) / \
            (df['vol']*df['adj_factor']).rolling(50).mean() - 1)
    #print(vol_chg)
    ma=dict()
    rt=dict()

    for i in N1List:
        ma[i] = ((df['close']*df['adj_factor']).rolling(i).mean()/df['adj_factor'])
        rt[i] = df['pct_chg'].rolling(i).sum()
        
    H = (df['high']*df['adj_factor']).expanding().max()
    
    HH = (H/df['adj_factor'])
    L = (df['low']*df['adj_factor']).expanding().min()
    LL = (L/df['adj_factor'])
    
    close = df['close']
    pct_chg = df['pct_chg']
    adj_factor = df['adj_factor']
       
    pct_ma_rt_oh = pd.DataFrame({
    'close_pre':close, 'pch_chg_pre': pct_chg, 'adj_factor_pre':adj_factor, 'vol_mean':vol_mean, 'vol_chg':vol_chg,
    'ma4':ma[4],'ma9':ma[9],'ma19':ma[19], 'ma59':ma[59],'ma119':ma[119],'ma249':ma[249],
    'rt4':rt[4],'rt9':rt[9],'rt19':rt[19],'rt59':rt[59],'rt119':rt[119], 'rt249': rt[249], 
    'H':HH,'L':LL, 
    })
    
    last_pct_ma_rt_oh = pct_ma_rt_oh.sort_values(by="trade_date")[-1:]
    return last_pct_ma_rt_oh


def get_stock_day_ma_rt_oh(ts_code=None,N=250)-> pd.DataFrame:
    try:
        data = RrdataD('stock_day_bfq_adj').read(instruments=ts_code, count=N)
    except:
        pass
    if isinstance(data, pd.DataFrame):
        if len(data) >= N:
            data.drop_duplicates(keep='last',inplace=True)
            data = data.sort_values(by="trade_date", ascending=True)
        
            i = 0
            while data.loc[0, 'vol'] == 0 :
                i += 120
                #print(i)
                data = RrdataD('stock_day_bfq_adj').read(instruments=ts_code, count=N+i)
                data.sort_values(by="trade_date", ascending=True)

            data.fillna(method='ffill',inplace=True)
            # save result
            data = data.iloc[-(N+1): ]
            #print(data)
    

        df = data.copy()
        vol_mean =  (df['vol']*df['adj_factor']).rolling(50).mean()
        vol_chg = 100*((df['vol']*df['adj_factor']) / \
                (df['vol']*df['adj_factor']).rolling(50).mean() - 1)
    
        ma=dict()
        rt=dict()

        for i in NList:
            ma[i] = ((df['close']*df['adj_factor']).rolling(i).mean()/df['adj_factor'])
            rt[i] = df['pct_chg'].rolling(i).sum()
            
        H = (df['high']*df['adj_factor']).expanding().max()
        HH = (H/df['adj_factor'])
        OH = 100*(df['close']/HH)
        L = (df['low']*df['adj_factor']).expanding().min()
        LL = (L/df['adj_factor'])
        OL = 100*(df['close']/LL -1)
        
        close = df['close']
        pct_chg = df['pct_chg']
        adj_factor = df['adj_factor']
        trade_date = df['trade_date']
        symbol = df['symbol']
        
        pct_ma_rt_oh = pd.DataFrame({
        'ts_code':ts_code,'trade_date': trade_date,'close':close, 'pch_chg': pct_chg, 'adj_factor':adj_factor, 'vol_chg':vol_chg,
        'ma5':ma[5],'ma10':ma[10],'ma20':ma[20], 'ma60':ma[60],'ma120':ma[120],'ma250':ma[250],
        'rt5':rt[5],'rt10':rt[10],'rt20':rt[20],'rt60':rt[60],'rt120':rt[120], 'rt250': rt[250], 'OH':OH,'OL':OL,
        'H':HH,'L':LL, 'symbol':symbol
        })
        
        #print(pct_ma_rt_oh)
        last_pct_ma_rt_oh = pct_ma_rt_oh.sort_values(by="trade_date")[-1:]
        #last_pct_ma_rt_oh.dropna(subset=['rt250'], inplace=True)
        return last_pct_ma_rt_oh
    
    else:
        return None
        
        
    

if __name__ == "__main__":
    import time 
    t1 = time.perf_counter()
    #main()
    #get_stock_day_ma_rt_pre('000792.SZ')
    #print(get_stock_day_ma_rt_oh(ts_code='688327.SH'))
    #print(RrdataD('stock_day_hfq_test').read(instruments='000001.SZ'))
    save_stock_day_ma_rt_oh_to_pgsql()
    t2 = time.perf_counter()
    print(f"{t2 - t1 = }")