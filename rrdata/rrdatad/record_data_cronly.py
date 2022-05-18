import time

from rrdata.rrdatad.rrdataD_save_api import RrdataDSave
from rrdata.rrdatad.stock import stock_zh_a_spot_em

data = stock_zh_a_spot_em()
data.sort_values(by="chg_pct", ascending=False,inplace=True)
print(data)

while 1:
    RrdataDSave('stock_spot').save(data)
    time.sleep(20)



