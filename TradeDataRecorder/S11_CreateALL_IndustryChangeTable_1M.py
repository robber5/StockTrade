# coding=utf-8


import pandas as pd
import sqlite3
from WindPy import *

import sys

reload(sys)

sys.setdefaultencoding('utf8')


table_name = 'stock_all_1M_all_factors_for_roll'
conn = sqlite3.connect('E:/Wanda_Work/sqlite/stock.db')
query_line = ("select DISTINCT [dateI] from %(table_name)s order by [dateI]" % {'table_name': table_name})
SQL_tuple = pd.read_sql_query(query_line, conn)

index_date_roll = pd.DataFrame(SQL_tuple, columns=['dateI'])
# index_date_roll.set_index('dateI', drop=False, inplace=True)
# print index_date_roll

industry_list = pd.read_csv('./Industry_IndexCode.csv')
industry_list.columns = ['industry', 'index_code', 'until']
print industry_list

w.start()

for j in range(0, len(industry_list)):
    clmnX = str(industry_list.industry[j])
    index_date_roll[clmnX] = ''
    index_code = str(industry_list.index_code[j])
    for i in range(1, len(index_date_roll)):
        dateNw = index_date_roll.dateI[i][0:10]
        datePr = index_date_roll.dateI[i-1][0:10]
        print str(index_code)+': '+str(datePr) + '---' + str(dateNw)
        data0 = w.wsd(str(index_code),'close',str(dateNw),str(dateNw))
        data1 = w.wsd(str(index_code),'close',str(datePr),str(datePr))
        C0 = data0.Data[0][0]
        C1 = data1.Data[0][0]
        print C0
        print C1
        if C1 is None:
            pass
        else:
            index_date_roll[clmnX][i] = C0/C1 - 1.0


w.stop()
# print index_date_roll
# index_date_roll.to_csv( './industry_change.csv', index=False)
index_date_roll.to_sql('industry_change', conn, if_exists='append', index=False, chunksize=100000)





