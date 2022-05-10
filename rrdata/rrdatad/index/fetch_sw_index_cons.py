#!/usr/bin/env bash

import pandas as  pd

#from .fetch_sw_index_class import sw_index_cons, sw_index_cons_all

class SwIndexCons(object):
    
    def __init__(self):
        self.LEVEL = ["L1","L2","L3"]


    def sw_index_cons(self, symbol: str = "851921.SI") -> pd.DataFrame:
        """
        乐咕乐股-申万一，二，三级-行业成份
        https://legulegu.com/stockdata/index-composition?industryCode=851921.SI
        :param symbol: 三级行业的行业代码
        :type symbol: str
        :return: 行业成份
        :rtype: pandas.DataFrame
        """
        url = f"https://legulegu.com/stockdata/index-composition?industryCode={symbol}"
        temp_df = pd.read_html(url)[0]
        temp_df.columns = [
            "no",
            "stock_code",
            "display_name",
            "in_time",
            "sw_L1",
            "sw_L2",
            "sw_L3",
            "close",
            "pe",
            "pe_ttm",
            "pb",
            "dy",
            "mv",
        ]
        temp_df["close"] = pd.to_numeric(temp_df["close"], errors="coerce")
        temp_df["pe"] = pd.to_numeric(temp_df["pe"], errors="coerce")
        temp_df["pe_ttm"] = pd.to_numeric(temp_df["pe_ttm"], errors="coerce")
        temp_df["pb"] = pd.to_numeric(temp_df["pb"], errors="coerce")
        temp_df["dy"] = pd.to_numeric(temp_df["dy"].str.strip("%"), errors="coerce")
        temp_df["mv"] = pd.to_numeric(temp_df["mv"], errors="coerce")
        return temp_df
    
      

def sw_index_cons(symbol: str = "851921.SI") -> pd.DataFrame:
    """
    乐咕乐股-申万一，二，三级-行业成份
    https://legulegu.com/stockdata/index-composition?industryCode=851921.SI
    :param symbol: 三级行业的行业代码
    :type symbol: str
    :return: 行业成份
    :rtype: pandas.DataFrame
    """
    url = f"https://legulegu.com/stockdata/index-composition?industryCode={symbol}"
    temp_df = pd.read_html(url)[0]
    temp_df.columns = [
        "no",
        "stock_code",
        "display_name",
        "in_time",
        "sw_L1",
        "sw_L2",
        "sw_L3",
        "close",
        "pe",
        "pe_ttm",
        "pb",
        "dy",
        "mv",
    ]
    temp_df["close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    temp_df["pe"] = pd.to_numeric(temp_df["pe"], errors="coerce")
    temp_df["pe_ttm"] = pd.to_numeric(temp_df["pe_ttm"], errors="coerce")
    temp_df["pb"] = pd.to_numeric(temp_df["pb"], errors="coerce")
    temp_df["dy"] = pd.to_numeric(temp_df["dy"].str.strip("%"), errors="coerce")
    temp_df["mv"] = pd.to_numeric(temp_df["mv"], errors="coerce")
    return temp_df


if __name__ == "__main__":
    print(sw_index_cons(symbol="801010.SI"))
    print(sw_index_cons(symbol="801012.SI"))
    print(sw_index_cons())




