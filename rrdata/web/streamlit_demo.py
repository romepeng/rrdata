#!/usr/bin/env python
import streamlit as st 

from rrdata.rrdatac.rrdataD_read_api import  RrdataD


df = RrdataD("stock_spot").read(fields="code,name,close,chg_pct,vol_chg,amount,pe,pb")

st.write(df)

