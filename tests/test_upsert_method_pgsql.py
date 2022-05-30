from rrdata.common.engine_pgsql import engine
from rrdata.utils.upsert_method import create_upsert_method
from rrdata.rrdatac.rrdataD_read_api import RrdataD
from rrdata.rrdatad.rrdataD_save_api import RrdataDSave
from rrdata.rrdatad.stock.fetch_stock_day import fetch_stock_day_bfq_from_tspro

df1 = RrdataD('stock_day_bfq').read(count=3)
print(df1)

df2 = fetch_stock_day_bfq_from_tspro()
print(df2) 

"""
df2.to_sql(
  table_name='stock_dat_bfq_test',
  db_engine=engine(),
  schema=db_schema,
  index=False,
  if_exists="append",
  chunksize=200, # it's recommended to insert data in chunks
  method=upsert_method
)
"""