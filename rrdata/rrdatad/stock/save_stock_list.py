from rrdata.rrdatad.stock.fetch_stock_list import fetch_stock_list_tusharepro
from rrdata.common import save_df_to_pgsql

save_df_to_pgsql(fetch_stock_list_tusharepro(), 'stock_list')


