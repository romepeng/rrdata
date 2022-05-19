from rrdata.rrdatad.stock.fetch_stock_list import fetch_stock_list_tusharepro
from rrdata.common import save_df_to_pgsql
from rrdata.rrdatad.stock.stock_zh_a_hist_em import stock_zh_a_spot_em

save_df_to_pgsql(fetch_stock_list_tusharepro(), 'stock_list')

#stock_spot = stock_zh_a_spot_em()

save_df_to_pgsql(stock_zh_a_spot_em(), 'stock_spot')


def rq_save_stock_list_pg():
    stock_list_l= pro.stock_basic(exchange_id='', is_hs='',list_status='L' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    #stock_list_D= pro.stock_basic(exchange_id='', is_hs='',list_status='D' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')  
    #stock_list_P= pro.stock_basic(exchange_id='', is_hs='',list_status='P' , fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')          
    #stock_list=pd.concat([stock_list_l,stock_list_D],axis=0)
    #stock_list=pd.concat([stock_list_l,stock_list_P],axis=0)
    stock_list = stock_list_l
    stock_list['code']=stock_list['symbol']
    #print(df)
    try:   
        t=time.time()    
        save_data_to_postgresql('stock_list',stock_list)
        t1=time.time()
        rq_util_log_info('save stock_list data success,take '+str(round(t1-t,2))+' S') 
    except Exception as e:        
        print(e) 