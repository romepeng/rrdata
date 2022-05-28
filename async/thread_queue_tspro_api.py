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
SENTINEL = "END"

def producer(queue):
    """Pretend we're getting stock_day hfq from  ts.bar_pro."""
    stock_list = RrdataD('stock_list').read().ts_code.values[:50]
    print(stock_list)
    for i, code in enumerate(stock_list):
        data = fetch_stock_daily_hfq_one_tspro(code)
        #print(data)
        RrdataDSave("stock_day_queue_test",if_exists='append').save(data)
        queue.put(i)
    queue.put(SENTINEL)


def consumer(queue):
    """Pretend we're saving a number in the database."""
    while True:
        item = queue.get()
        if item != SENTINEL:
            print(f"Retrieve element {item}")
            queue.task_done()
        else:
            print("Receive SENTINEL, the consumer will be closed.")
            queue.task_done()
            break
        
       


def main():
   
    threads = [threading.Thread(target=producer, args=(q,)),threading.Thread(target=consumer, args=(q,)),]

    for thread in threads:
        thread.start()

    q.join()

    #print(RrdataD('stock_day_queue_test').read())
    
    
if __name__ == "__main__":
    t1 =time.perf_counter() 
    main()
    t2 = time.perf_counter()
    print(f"{ t2 - t1}")
   
      
    
    
  