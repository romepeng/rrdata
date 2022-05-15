#!/usr/bin/python
""" see bigquant doc
    DataSource("basic_info_IndustrySw").read(instruments=stock_lists)
    out: DataFrame
"""

#from rrdatad import DataSource

class DataSource(object):

    def __init__(self):
        self.basic_info_IndustrySw = basic_info_IndustrySw
    
    
    def read(self, instruments="",start_date="",end_date="",fields=[]):

        return 1



def basic_info_IndustrySw():
    pass


# dict(key,values)
# func("name")

dict_func = dict(zip(['func_name'],['func']))

print(dict_func)



