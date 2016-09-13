# coding=utf-8

import pandas as pd

df = pd.read_csv('volatility-1m.csv')

df = df.dropna(how='all')

df = df.loc[:, ['品种', '客户买', '客户卖', '交易门槛']]

listIns = df['品种'].values

n = ''

for ins in listIns:

    if ins[-3:] == 'CZC' and ins[:2].isalpha():
        n += ins[:2] + '1' + ins[-7:-4] + ','
    else:
        n += ins[:-4] + ','


print n[:-1]
