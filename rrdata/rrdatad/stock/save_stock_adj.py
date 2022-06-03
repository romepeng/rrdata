from rrdata.common.tusharepro import pro
from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.rrdatad.rrdataD_save_api import RrdataDSave
from rrdata.utils.rqParameter import startDate
from rrdata.utils.rqDate_trade import rq_util_get_last_tradedate,rq_util_get_trade_range


def save_stock_adjfactor_to_pgsql(start_date=startDate, end_date=None, table_name="stock_adjfactor"):
    """ update pro.adj from start_date to end_date to pgsql"""
    if not end_date:
        end_date = rq_util_get_last_tradedate()
    print(f"{start_date=}, {end_date=}, trade_days:{len(rq_util_get_trade_range(start_date,end_date))}")
    try:
        trade_data_pg = RrdataD(table_name).read(fields=['trade_date']).trade_date.to_list() 
    except: 
        #第一次运行
        trade_data_pg = []
    finally:
        trade_date_sql = trade_data_pg
    #print(f"sql_trade_date nums:{len(trade_date_sql)}, like :{trade_date_sql[-1:]}")  # read trade_adte from stock_day_bfq 
    trade_date = list(map(lambda x: x.replace("-",""), rq_util_get_trade_range(start_date, end_date)))  # TODO
    #print(len(trade_date))
    trade_date2=list(set(trade_date).difference(set(trade_date_sql)))  #差集 trade_date in / not in trade_date_sql
    # trade_date2 = [x for x in trade_date if x not in trade_date_sql]
    trade_date2.sort()
    print(f" Want to add trade_date data nums : {len(trade_date2)}")
    
    if len(trade_date2)==0:
        print(f'<{table_name}> data is up to date !')
    for i in trade_date2:
        print(i)
        try:
            df = pro.query('adj_factor', trade_date=i.replace("-",""))        
            #print(df)
            RrdataDSave(table_name, if_exists='append').save(df)
        except Exception as e:
            print(e)


if  __name__ == "__main__":
    import time
    t1= time.perf_counter()
    save_stock_adjfactor_to_pgsql()
    t2 = time.perf_counter()
    print(f" {t2 - t1 = }")
    
    
    