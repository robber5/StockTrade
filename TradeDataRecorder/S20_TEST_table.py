# coding=utf-8

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import sys
import math

reload(sys)

sys.setdefaultencoding('utf8')


def standardize_f(fctor):
    # 因子1准备
    X0 = fctor.dropna()
    X1 = fctor.copy()
    # #因子去极值处理
    m_X = np.median(X0)
    mad_X = np.median(abs(X0-m_X))
    thresh_max = m_X+5 * mad_X
    thresh_min = m_X-5 * mad_X
    X1[X1 > thresh_max] = thresh_max
    X1[X1 < thresh_min] = thresh_min
    # 因子值标准化
    u_X = np.mean(X1.dropna())
    std_X = np.std(X1.dropna())
    X1 = (X1-u_X)/std_X
    return X1

table_name = 'stock_all_1M_all_factors_for_weight'
sql_engine = create_engine('sqlite:///E:\Wanda_Work\sqlite\stock.db')
query_line = ("select * from %(table_name)s order by [dateI]" % {'table_name': table_name})
dfFactor = pd.read_sql(query_line, sql_engine)
dfFactor.set_index('dateS', drop=False, inplace=True)
# print dfFactor

# 所有因子列做标准化处理
# for i in range(4, len(dfFactor.T)-5):
#     print dfFactor.iloc[:, i]
#     dfFactor.iloc[:, i] = standardize_f(dfFactor.iloc[:, i])

# 另建 Rolling dataframe
dfRollingFactor = dfFactor.loc[:, ['code','dateS','dateI', 'stock_change']].copy()
# print dfRollingFactor

# 计算加权后的因子
# Earnings_Yield
dfRollingFactor['ETOP'] = dfFactor['ETOP']
dfRollingFactor['ETP5'] = dfFactor['ETP5']
# print dfRollingFactor

# Growth
dfRollingFactor['Growth'] = dfFactor['AGRO']

# Leverage
dfRollingFactor['Leverage'] = dfFactor['MLEV']

# Liquidity
dfRollingFactor['STO_1M'] = dfFactor['STO_1M']
dfRollingFactor['STO_3M'] = dfFactor['STO_3M']
dfRollingFactor['STO_6M'] = dfFactor['STO_6M']
dfRollingFactor['STO_12M'] = dfFactor['STO_12M']
dfRollingFactor['STO_60M'] = dfFactor['STO_60M']

# Momentum
dfRollingFactor['ALPHA'] = dfFactor['ALPHA']
dfRollingFactor['RSTR_1M'] = dfFactor['RSTR_1M']
dfRollingFactor['RSTR_3M'] = dfFactor['RSTR_3M']
dfRollingFactor['RSTR_6M'] = dfFactor['RSTR_6M']
dfRollingFactor['RSTR_12M'] = dfFactor['RSTR_12M']


# Size
dfRollingFactor['Size'] = dfFactor['LNCAP']

# Value
dfRollingFactor['BTOP'] = dfFactor['BTOP']
dfRollingFactor['STOP'] = dfFactor['STOP']

# Volatility
dfRollingFactor['HILO'] = dfFactor['HILO']
dfRollingFactor['BTSG'] = dfFactor['BTSG']
dfRollingFactor['DASTD'] = dfFactor['DASTD']
dfRollingFactor['LPRI'] = dfFactor['LPRI']
dfRollingFactor['CMRA'] = dfFactor['CMRA']
dfRollingFactor['VOLBT'] = dfFactor['VOLBT']
dfRollingFactor['SERDP'] = dfFactor['SERDP']
dfRollingFactor['BETA'] = dfFactor['BETA']
dfRollingFactor['LPRI'] = dfFactor['LPRI']
dfRollingFactor['SIGMA'] = dfFactor['SIGMA']


# Financial_Quality
dfRollingFactor['S_GPM'] = dfFactor['S_GPM']
dfRollingFactor['C_GPM'] = dfFactor['C_GPM']
dfRollingFactor['T_GPM'] = dfFactor['T_GPM']
dfRollingFactor['S_NPM'] = dfFactor['S_NPM']
dfRollingFactor['C_NPM'] = dfFactor['C_NPM']
dfRollingFactor['T_NPM'] = dfFactor['T_NPM']
dfRollingFactor['S_ROE'] = dfFactor['S_ROE']
dfRollingFactor['C_ROE'] = dfFactor['C_ROE']
dfRollingFactor['T_ROE'] = dfFactor['T_ROE']
dfRollingFactor['S_ROA'] = dfFactor['S_ROA']
dfRollingFactor['C_ROA'] = dfFactor['C_ROA']
dfRollingFactor['T_ROA'] = dfFactor['T_ROA']


# 对齐 行业 和 行业指数收益率
query_line = ("select * from stock_all_1M_industry_concept order by [dateS]")
dfStockIndustry = pd.read_sql(query_line, sql_engine)
dfStockIndustry.set_index('dateS', drop=False, inplace=True)

df = pd.merge(dfRollingFactor, dfStockIndustry, how='left',on=['code','dateS'])
df.set_index('dateI', drop=False, inplace=True)

# print df

query_line = ("select * from industry_change order by [dateI]")
dfIndustryChange = pd.read_sql(query_line, sql_engine)
dfIndustryChange.set_index('dateI', drop=False, inplace=True)
# print dfIndustryChange

df[ 'IndustryChange'] = None
for dateI in df.dateI.unique():   #len(df)):
    for industry in df.industry.unique():
        print str(dateI) + ':  ' + str(industry)
        if isinstance(industry, basestring):    # 判断是否 字符型
            if (industry is not None):
                df.loc[(df['dateI']==dateI) & (df['industry']==industry), 'IndustryChange'] = dfIndustryChange.loc[dateI, industry]

# print df

conn = sqlite3.connect('E:/Wanda_Work/sqlite/stock.db')
df.to_sql('stock_all_F_1M_weighting', conn, if_exists='append', index=False, chunksize=100000)