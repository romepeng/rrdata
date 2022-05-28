# coding:utf-8
""" df.to_csv(table_name.csv, encoding="utf_8_sig)"""
from bs4 import BeautifulSoup
import requests
import time
import os
from threading import Thread
import random

cur_path = os.path.dirname(os.path.realpath(__file__))
print(cur_path)

 
class CountDownTask:
    def __init__(self):
        self._running = True

    def terminate(self):
        self._running = False

    def run(self, n):
        while self._running and n > 0:
            print('T-minus', n)
            n -= 1
            time.sleep(random.uniform(0.5,1.5))
     

if __name__ == '__main__':
    c = CountDownTask()
    t = Thread(target=c.run, args=(5,))
    t.start()
    #c.terminate()
    #t.join()

