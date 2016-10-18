# coding=utf-8

"""
filename:dataAnalysis
Author：zyf
LastEditTime:2016/9/29
"""

import pandas as pd

dfZig = pd.read_csv('sz000885dfZig.csv')
dfStock = pd.read_csv('dfATR.csv').loc[:, ['code', 'date', 'close','preclose']]

print(dfZig[dfZig['date'] <= '2015-1-1']).sort_values(by='date', ascending=False).head(6)['price'].values

print(max((dfZig[dfZig['date'] <= '2015-1-1']).sort_values(by='date', ascending=False).head(6)['price'].values))

