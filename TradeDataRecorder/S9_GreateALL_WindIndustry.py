# coding=utf-8


import pandas as pd
import sqlite3
from WindPy import *

table_name = 'stock_all_1M_all_factors_for_roll'

conn = sqlite3.connect('E:/Wanda_Work/sqlite/stock.db')

query_line = ("select [code],[dateS] from %(table_name)s where dateS>='2000-01-01' \
              and dateS<'2016-06-01' order by dateS desc"
              % {'table_name': table_name})
df = pd.read_sql_query(query_line, conn)
df['industry'] = ''
df['industry_code'] = ''
df['concept'] = ''
# [industry],[industry_code],[concept]
# print df

dftmp = df.head(1).copy()
# print dftmp

w.start()

if w.isconnected():

    for i in range(0,len(df)):
        stock = df.code[i][2:8] + '.' + df.code[i][0:2].upper()
        date = df.dateS[i][0:10]
        print stock + '  :  ' + date

        # stock = '601628.SH'
        data = w.wsd(stock, "concept,industry2,industrycode",date,date,"industryType=1","industryStandard=1","Period=M","PriceAdj=B")

        df.concept[i] = data.Data[0][0]
        df.industry[i] = data.Data[1][0]
        df.industry_code[i] = data.Data[2][0]
        # print df.iloc[i,:]
        dftmp.code[0]     = df.code[i]
        dftmp.dateS[0]    = df.dateS[i]
        dftmp.concept[0]  = df.concept[i]
        dftmp.industry[0]    = df.industry[i]
        dftmp.industry_code[0]  = df.industry_code[i]
        # print dftmp
        # print data.Data[0][0]
        # print data.Data[1][0]
        # print data.Data[2][0]
        # print df
        dftmp.to_sql('stock_all_1M_industry_concept', conn, if_exists='append', index=False, chunksize=100000)

w.stop()

# df.to_sql('stock_all_1M_industry_concept', conn, if_exists='append', index=False, chunksize=100000)

# print df
