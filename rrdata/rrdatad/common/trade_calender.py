from rrdata.utils.rqSetting import setting
from rrdata.utils.rqTusharepro import ts
from rrdata.utils.rqDate_trade import rq_util_format_date2str


def get_trade_date_cn():
    pass


def get_trade_date_sse_tspro():
    token =  setting['TSPRO_TOKEN']
    pro = ts.pro_api(token=token)
    df = pro.trade_cal(exchange='SSE', start_date='20050101', end_date='20251231') #'19900101'
    trade_date = df[df.is_open == 1]['cal_date'].values
    trade_date = list(map(lambda x: rq_util_format_date2str(x),  trade_date))
    return trade_date
    
trade_date_sse = get_trade_date_sse_tspro()


