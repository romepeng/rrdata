from rrdata.rrdatad.trade_calender.trade_date_hist import trade_date_hist_sina

df = trade_date_hist_sina()
print(df)
print(df.trade_date.values)
print(type(df.trade_date.values[-1]))

