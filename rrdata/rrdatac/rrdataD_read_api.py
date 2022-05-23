#!/usr/bin/python
""" see bigquant doc
    DataSource('表名').read(instruments,start_date='2005-01-01', end_date=None, fields=['字段1','字段2'...])
    DataSource("basic_info_IndustrySw").read(instruments=stock_lists)
    参数 :
    表名  不同的表名存放了不同字段的历史数据，见 数据表名
    instruments  字符串列表，股票代码列表，见 股票列表
    start_date  字符串，开始日期
    end_date  字符串，结束日期
    fields  str 字符串数组，请求的字段列表，详见 数据字段
    返回:
    历史数据
    DataFrame
"""
from dataclasses import fields
from re import I
import pandas as pd
from typing import List, TypeVar

from pandas import DataFrame
from rrdata.common import read_df_from_table, save_df_to_pgsql
from rrdata.common.engine_pgsql import engine


class RrdataD(object):
    """ see:
        bigquant DataSource("trade_days").read() 
        also add  can change:
        source database/sql_driver
        SELECT DISTINCT --only
    """
    def __init__(self,
                table_name, 
                datasource=engine(driver="", db_name="rrdata"),
                ):
        """ driver: ""-default - poasgresql/ psycopg2   db_name  rrdata/rrshare/rrfactor"""
        self.table_name = table_name
        self.engine = datasource

    def __repr__(self) -> str:  # name 
        return f"<{self.__class__.__name__} table_name={self.table_name}>"

    def read(self, instruments: List=None,start_date: str=None, end_date: str=None,fields: List=None) -> DataFrame:
        """   default None (all)  --> out whole table """
        if fields:
            field =  ','.join(fields) if isinstance(fields, list)  else fields.replace(' ','') 
        else:
            field ="*"
        sql_query = f"""SELECT DISTINCT {field}  
                        FROM {self.table_name}
                        """
        if start_date:
            sql_query += f"WHERE trade_date >= '{start_date}' "
        if end_date:
            sql_query += f"AND trade_date <= '{end_date}' "
        if instruments:
            iterms = "AND" if start_date else "WHERE" 
            if isinstance(instruments, list):
                instrument = "','".join(instruments)
            else:
                instruments = instruments.replace(' ','')
                instrument = "','".join(instruments.split(",")) #map(lambda x: ",".join('x'), instruments)
            sql_query +=  f"{iterms} ts_code in ('{instrument}')"
        #print(sql_query)
        try:
            return pd.read_sql(sql_query, self.engine)
        except Exception as E:
            print(E)


if  __name__ == "__main__":
    print(RrdataD("swl_list").__repr__())
    print(RrdataD('stock_list').read())
    #print(RrdataD('stock_list_tspro').read(fields=["ts_code", "name", "list_status"]))
    #print(RrdataD('stock_spot').read(fields="code,pb, pe"))
    print(RrdataD('stock_day_test').read(start_date='2022-05-16', fields="ts_code , trade_date,close"))
    #print(RrdataD('stock_day_test').read(instruments=["000792.SZ", "600519.SH"]))
    #print(RrdataD('stock_day_test').read(instruments=" 000792.SZ , 600519.SH "))

    #print(RrdataD('stock_day_test').read(start_date='2022-05-16'))
