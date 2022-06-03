from tracemalloc import start
import pandas as pd
import easyquotation as eq

import efinance as ef

from rrdata.common.tusharepro import pro
from rrdata.utils.rqDate import rq_util_date_today
from rrdata.utils.rqDate_trade import rq_util_get_last_tradedate, trade_date_sse
from rrdata.rrdatad.stock.fetch_stock_basic_tspro import fetch_delist_stock,fetch_stock_list

from rrdata.rrdatad.stock.save_stock_day import fetch_stock_day_bfq_from_tspro
from rrdata.utils.rqParameter import startDate

df = ef.stock.get_realtime_quotes()
df = df[['股票代码',  '涨跌幅', '最新价', '最高', '最低', '今开', '成交量', '成交额', '昨日收盘','市场类型']]
df = df.rename(columns={'股票代码':'symbol',  '涨跌幅':'chg_pct', '最新价':'close', '最高':'high', '最低':'low', '今开':'open', 
                   '成交量':'vol', '成交额':'amount', '昨日收盘':'pre_close','市场类型':'mk_type'})
print(df)
print(df.columns)
print(set(df.mk_type.values))

codes = fetch_stock_list(df=False, ts_code=False)[:10]
print(codes)
df = ef.stock.get_quote_history(codes,beg=startDate)


def fetch_stock_spot_adj(src='sina'): # TODO
    """if today is trade date trade_date = today
        else trade_date = last trade_date
        time: 9:30-11:31 , 13:00-15:03
        pro.adj_factor update at 9:30
        # TODO BSE 
    """
    
    trade_date = rq_util_date_today() if rq_util_date_today().strftime('%Y-%m-%d') \
        in trade_date_sse else  rq_util_get_last_tradedate()
    trade_date = str(trade_date).replace('-','')
    quotation = eq.use(src)
    df_factor=pro.query('adj_factor',trade_date=trade_date)
    df_factor['code'] =  df_factor['ts_code'].apply(lambda x: x[0:6])
    all_secs = df_factor['ts_code'].values
    secs_eq = list(map(lambda x: x[0:6], all_secs))
    price_all = quotation.stocks(secs_eq)
    df_p = pd.DataFrame(price_all).T.reset_index()
    df_p = df_p[['index','name', 'close','now','open','high','low','turnover','volume','date','time']]
    df_p = df_p.loc[df_p['volume'] > 0]
    df_p['pct_chg'] = (100*(df_p['now']/df_p['close'] - 1)).map(lambda x:round(x,2))
    df_p['avg'] = df_p['volume']/df_p['turnover']
    df_p = df_p.rename(columns={'index':'code','close':'pre_close', 'now':'close','turnover':'vol','volume':'amount'})
    df_p = df_p.sort_values(by='code')
    for i in ['vol', 'amount']:
        df_p[i] = (df_p[i]/100.00).apply(lambda x: round(float(x), 2))
    df = pd.merge(df_factor, df_p, how='left', on='code').sort_values(by = 'code')
    #df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
    delist_code = fetch_delist_stock(trade_date)
    df=df[~df['code'].isin(delist_code)]
    
    df.fillna({'pct_chg':0,'vol':0,'amount':0}, inplace=True)
    return df

if __name__ == "__main__":
    #print(fetch_stock_spot_adj())
    pass