import concurrent.futures
import logging
import queue
import random
import threading
import time
from timeit import timeit

from rrdata.rrdatad.stock.tusharepro import ts, pro
from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.rrdatad.rrdataD_save_api import RrdataDSave
from rrdata.rrdatad.stock.fetch_stock_day import fetch_stock_list_tspro,fetch_stock_daily_hfq_one_tspro


q = queue.Queue()

workers = []
#df = RrdataD('stock_list').read()
stock_list = RrdataD('stock_list').read().ts_code.values
#print(stock_list)
NUM_THREADS = 5
NUM = len(stock_list) // NUM_THREADS
print(NUM)

df = RrdataD('stock_list').read()
df.sort_values(by="ts_code", inplace=True)
print(df.head())
stock_list = df.ts_code.values
#stock_dict = df['ts_code'].to_dict()
print(stock_list[1])

def get_stock_day_save_tosql(queue):
    id = queue.get()
    print(f" start get id : ---- {id}")
    data = fetch_stock_daily_hfq_one_tspro(stock_list[id])
    #print(data)
    RrdataDSave("stock_day_queue_2",if_exists='append').save(data)



def main():
    for i in range(NUM_THREADS + 1):
        for j in range(NUM):
            worker = threading.Thread(target=get_stock_day_save_tosql, args=(q,))
            worker.start()
            workers.append(worker)
            if (i * NUM + j) >= len(stock_list):
                break
        
    for i in range(NUM_THREADS + 1):
        #t1 = time.perf_counter()
        for j in range(NUM):
            id = i * NUM + j
            print(id)
            time.sleep(1)
            q.put(id)
            if id >= len(stock_list):
                break
        #t = time.perf_counter() - t1
        #print(t)
        #time.sleep(1)
    for w in workers():
        w.join()
        
   

if __name__ == "__main__":
    t1 = time.perf_counter() 
    main()
    t2 = time.perf_counter()
    
    #print(RrdataD('stock_day_queue_2').read(instruments='000002.SZ'))
    print(f"{ t2 - t1}")
    
    
  