from rrdata.rrdatac.rrdataD_read_api import RrdataD


print(RrdataD("adj_factor").read(instruments="000792.SZ,600519.SH,000002.SZ,000001.SZ",start_date='2022-05-17',end_date='2022-05-20'))
print(RrdataD("adj_factor").read(start_date='2022-05-16'))
print(RrdataD("adj_factor").read(instruments='000792.SZ'))