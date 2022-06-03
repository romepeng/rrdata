from rrdata.rrdatad.stock.save_stock_day import save_stock_day_bfq_adj_to_pgsql, save_stock_day_hfq_to_pgsql
from rrdata.rrdatad.stock.save_stock_list import save_stock_list_to_pgsql
from rrdata.rrdatad.stock.save_stock_adj import save_stock_adjfactor_to_pgsql

def record_stock_list_to_pgsql():
    save_stock_list_to_pgsql()


def record_stock_adjfactor_to_pgsql():
    save_stock_day_bfq_adj_to_pgsql()
    
def record_stock_day_bfq_adj_to_pgsql():
    save_stock_day_bfq_adj_to_pgsql()


def record_stock_day_hfq_to_pgsql():
    save_stock_day_hfq_to_pgsql()
    

save_stock_list_to_pgsql()
save_stock_adjfactor_to_pgsql()
save_stock_day_bfq_adj_to_pgsql()

#save_stock_day_hfq_to_pgsql()

