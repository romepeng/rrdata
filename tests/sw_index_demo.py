# sw_index
from rrdata.rrdatad.index import (sw_index_cons,sw_index_class_all,
                                sw_index_class)

#print(sw_index_spot(level="L1"))
#print(sw_index_daily(symbol="801012",start_date="2022-01-01",end_date="2022-05-10"))
#print(sw_index_daily_indicator("801012","2022-04-01","2022-06-01"))


print(sw_index_cons("801012.SI"))
print(sw_index_class_all())

