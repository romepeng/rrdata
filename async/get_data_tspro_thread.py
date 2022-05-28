# coding=utf-8
import multiprocessing
    
from rrdata.utils.rqTusharepro import ts, pro
    
    # func: times/min
THROTTLE_RATES = {
    'daily': 500,
    'adj_factor': 500
}
    
    
def __get_stock_daily(self, start_date='19700101', max_worker=multiprocessing.cpu_count() * 2):
    df_cal_date = StockCalendar().query(
            fields='distinct cal_date',
            where=f'is_open=\'1\' and cal_date >=\'{start_date}\' and cal_date < \'{tomorrow()}\'',
            order_by='cal_date')
 
    with ThreadPoolExecutor(max_worker) as executor:
        future_to_date = \
                {executor.submit(self.__get_stock_daily_internal, ts_code='', trade_date=row['cal_date']): row
                 for index, row in df_cal_date.iterrows()}
        for future in as_completed(future_to_date):
            row = future_to_date[future]
            try:
                data = future.result()
            except Exception as ex:
                self.logger.error(f"failed to retrieve {row['cal_date']}")
                self.logger.exception(ex)
                    
class ThrottleDataApi(object):
     
    class RequestRecord(object):
        def __init__(self, rate):
            self.request_times = []
            self.throttle_rate = rate
            self.event = threading.Event()
            self.reach_limit = False
            self.lock = threading.RLock()
            self.event.set()
 
    # func: ([requesttime...], rate, event)
    __request_records = {}
 
    def __init__(self, api=ts.pro_api()):
        self.api = api
        for key, rate in config.THROTTLE_RATES.items():
            ThrottleDataApi.__request_records[key] = ThrottleDataApi.RequestRecord(rate)
 
    def __getattr__(self, name):
        self.__allow_request(name)
        return partial(getattr(self.api, name))
 
    def __allow_request(self, name):
        record = ThrottleDataApi.__request_records.get(name)
 
        def timer_callback(request_record):
            event.set()
            request_record.reach_limit = False
 
        if record:
            history = record.request_times
            rate = record.throttle_rate
            event = record.event
 
            while history and history[-1] <= time.time() - 60:
                history.pop()
            if len(history) >= rate:
                with record.lock:
                    if not record.reach_limit:
                        event.clear()
                        waiting_seconds = 60 + history[-1] - time.time() + 1
                        threading.Timer(waiting_seconds, timer_callback, [record]).start()
                        record.reach_limit = True
 
            event.wait()
            history.insert(0, time.time())
            