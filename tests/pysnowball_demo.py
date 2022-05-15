import pandas as pd
import pysnowball as ball

ball.set_token('xq_a_token=398416c78601a9480af83e98629091734a56f778;')
for code in ["SZ000792","SH600519","SZ002236","SZ000858","SH601952"]:
    earnforcast = ball.earningforecast(code)
    #print(earnforcast)
    print(pd.DataFrame(earnforcast['list']))

print(pd.DataFrame(ball.quotec('SZ000792')['data']))

print(ball.quote_detail("SZ000792"))
print(ball.report('SZ000792'))

code1= "SZ000792"
#print(ball.blocktrans('SZ002027'))
#print(ball.margin(code1))
#print(ball.business(code1))
#print(ball.business_analysis(code1))
print(ball.org_holding_change(code1))

#print(ball.watch_list())

print(ball.index_weight_top10("399967"))
print(ball.index_weight_top10("399814"))

# https://www.csindex.com.cn/#/indices/family/list
