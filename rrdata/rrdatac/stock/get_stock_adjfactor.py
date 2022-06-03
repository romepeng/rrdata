from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.utils import rq_util_get_last_tradedate, rq_util_get_pre_trade_date


count = 5
last_tradedate = rq_util_get_last_tradedate()
start_date = rq_util_get_pre_trade_date(last_tradedate, count)

def get_stock_adjfactor(instruments=None,trade_date=None,start_date=None, end_date=None,count=None,
                        fields="trade_date,ts_code,adj_factor"):
    return RrdataD("stock_day_hfq").read(instruments=instruments,trade_date=trade_date,
                                         start_date=start_date,
                                         end_date=end_date,fields=fields)

if __name__ == "__main__":
    print(get_stock_adjfactor(instruments='000792.SZ'))
    print(get_stock_adjfactor(trade_date="20220527"))

