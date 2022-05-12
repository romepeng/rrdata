# sw_index
"""
from rrdata import (sw_index_spot, sw_index_daily, 
                    sw_index_daily_indicator,
                    sw_index_class, 
                    sw_index_class_all,
                    sw_index_cons,
                    )
"""
#import rrdata.rrdatad.index as index
from rrdata.rrdatad import index

from rrdata.rrdatad.stock import version

print(index.sw_index_spot(level="L1"))
print(index.sw_index_daily(symbol="801012",start_date="2022-01-01",end_date="2022-05-10"))
print(index.sw_index_daily_indicator("801012","2022-04-01","2022-06-01"))
print(index.sw_index_cons("801011.SI"))
print(index.sw_index_class())

print(version)
