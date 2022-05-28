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
            if (i * NUM + j - 1) >= len(stock_list):
                break
        
    for i in range(NUM_THREADS + 1):
        t1 = time.perf_counter()
        for j in range(NUM):
            id = i * NUM + j
            print(id)
            q.put(id)
            if id >= len(stock_list):
                break
        t = time.perf_counter() - t1
        time.sleep(60 - t)
        
    for w in workers:
        w.join()
        

if __name__ == "__main__":
    main()
   
    print(RrdataD('stock_day_queue_2').read(instruments='000002.SZ'))
    
    
  