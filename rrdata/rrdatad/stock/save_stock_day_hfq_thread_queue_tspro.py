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

from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.rrdatad.rrdataD_save_api import RrdataDSave
from rrdata.rrdatad.stock.fetch_stock_day import fetch_stock_list_tspro,fetch_stock_daily_hfq_one_tspro
from rrdata.utils.rqSetting import setting



q = queue.Queue()
workers = []
NUM_THREADS = 25


df = RrdataD('stock_list').read()
df.sort_values(by="ts_code", inplace=True)
print(df.head())
stock_list = df.ts_code.values
#stock_dict = df['ts_code'].to_dict()
#print(stock_list[1])
NUM = len(stock_list) // NUM_THREADS
print(NUM)


def get_stock_day_save_tosql(queue, table_name='stock_day_hfq'):
    id = queue.get()
    print(f"get id : -- {id}")
    data = fetch_stock_daily_hfq_one_tspro(stock_list[id])
    #print(data)
    RrdataDSave(table_name,if_exists='append').save(data)


def main():
    for i in range(NUM_THREADS + 1):
        for j in range(NUM):
            worker = threading.Thread(target=get_stock_day_save_tosql, args=(q,))
            worker.start()
            workers.append(worker)
            if (i * NUM + j) >= len(stock_list):
                break
            else:
                continue
            
    for i in range(NUM_THREADS + 1):
        t1 = time.perf_counter()
        
        for j in range(NUM):
            id = i * NUM + j
            #print(id)
            q.put(id)
            if (id + 2) >= len(stock_list):
                break
            else:
                continue
            
        t = time.perf_counter() - t1
        if t < 60:
            time.sleep(60 - t)
        else:
            continue
        
    for w in workers:
        w.join()
        

if __name__ == "__main__":
    main()
   
    print(RrdataD('stock_day_hfq').read(instruments='000792.SZ'))