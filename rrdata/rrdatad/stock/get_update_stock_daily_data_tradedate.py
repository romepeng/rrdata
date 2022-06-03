from tracemalloc import start
from typing import List
from rrdata.common.tusharepro import pro
from rrdata.rrdatac.rrdataD_read_api import RrdataD

from rrdata.utils.rqParameter import startDate
from rrdata.utils.rqDate_trade import rq_util_get_last_tradedate,rq_util_get_trade_range
from rrdata.rrdatad.rrdataD_save_api import RrdataDSave

class UpdateDailyData:
    def __init__(self):
        self.start_date=None #:str=startDate
        self.end_date=None #:str=rq_util_get_last_tradedate()
        self.table_name=None    

    def want_update_stock_daily_data_to_pgsql_tradedate(self) -> List:
        """ update stock daily data from start_date to end_date to pgsql"""
        
        print(f"{self.start_date=}, {self.end_date=}, trade_days:{len(rq_util_get_trade_range(self.start_date,self.end_date))}")
        
        try:
            trade_data_pg = RrdataD(self.table_name).read(fields=['trade_date']).trade_date.to_list() 
        except: 
            #第一次运行
            trade_data_pg = []
        finally:
            trade_date_sql = trade_data_pg
        #print(f"sql_trade_date nums:{len(trade_date_sql)}, like :{trade_date_sql[-1:]}")  # read trade_adte from stock_day_bfq 
        trade_date = list(map(lambda x: x.replace("-",""), rq_util_get_trade_range(self.start_date, self.end_date)))  # TODO
        #print(len(trade_date))
        trade_date2=list(set(trade_date).difference(set(trade_date_sql)))  #差集 trade_date in / not in trade_date_sql
        # trade_date2 = [x for x in trade_date if x not in trade_date_sql]
        trade_date2.sort()
        print(f" Want to add trade_date data nums : {len(trade_date2)}")
        
        return trade_date2


def update_data_to_pgsql(self, data):
    update_tradedate = self.want_update_stock_daily_data_to_pgsql_tradedate()
    if len(update_tradedate)==0:
        print(f'<{self.table_name}> data is up to date !')
    for i in update_tradedate:
        print(i)
        try:
            #data = pro.query('adj_factor', trade_date=i.replace("-",""))        
            #print(data)
            RrdataDSave(self.table_name).save(data)
        except Exception as e:
            print(e)   
    


if  __name__ == "__main__":
    import time
    t1= time.perf_counter()
    
    UpdateDailyData(start_date='2019-05-01',end_date='',table_name='stock_adjfactor').want_update_stock_daily_data_to_pgsql_tradedate() 
    t2 = time.perf_counter()
    print(f" {t2 - t1 = }")
    
    
    