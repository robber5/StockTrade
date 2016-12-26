# coding=utf-8
import datetime
import pymssql
import pandas as pd
import numpy as np
from statsmodels import regression
import statsmodels.api as sm
import matplotlib.pyplot as plt
import math
import os

class MSSQL:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def ExecQuery(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecQueryPara(self, sql, para):
        cur = self.__GetConnect()
        cur.execute(sql, para)
        resList = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self, sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

def calculate_SERDP(x):  # 计算lags阶以内的自相关系数，返回lags个值，分别计算序列均值，标准差
    n = len(x)
    x = np.array(x)
    numrt = 0.0
    denmt = 0.0
    for i in range(0, n-2):
        numrt += (x[i]+x[i+1]+x[i+2])*(x[i]+x[i+1]+x[i+2])
        denmt += (x[i]*x[i]+x[i+1]*x[i+1]+x[i+2]*x[i+2])

    return numrt/denmt

@profile
def ss():
    # 查找已有文件
    dir = './month_factors'
    files = os.listdir(dir)
    file_list = []
    for name in files:
        stock_code = name[0:8]
        file_list.append(stock_code)

    print file_list

    # 连接数据库
    ms = MSSQL(host="localhost", user="sa", pwd="windows-999", db="stocks")

    # 读取股票列表
    SQL_tuple = ms.ExecQuery("SELECT DISTINCT [股票代码] FROM [stocks].[dbo].[all_financial_data] ")
    stock_list = pd.DataFrame(SQL_tuple, columns=['stock_code'])

    # 循环计算
    for stock_code in stock_list.stock_code:

        if stock_code in file_list:  # 如果已经有结果文件. 继续
            continue

        if stock_code[0:2] == 'sh':
            index_code = 'sh000001'
        elif stock_code[0:2] == 'sz':
            index_code = 'sz399001'

        print stock_code + '---< ' + index_code

        # 获取股票和指数月收益率数据
        SQL_tuple = ms.ExecQueryPara("select * from dbo.v_stock_1m a left join v_index_1m b on a.stock_date = b.index_date \
        where code =%s and index_code = %s order by a.stock_date DESC ", (stock_code, index_code))
        R_1M_data = pd.DataFrame(SQL_tuple,
                                 columns=['code', 'stock_trade_date', 'adjust_price',  'stock_date',  \
                                          'index_code', 'index_trade_dateI', 'index_close', 'index_date'])
        R_1M_data.set_index('stock_date', drop=False, inplace=True)  # lastdate 列设为index
        # print R_1M_data
        for i in range(0, len(R_1M_data)-1):
            R_1M_data.loc[R_1M_data.index[i], 'stock_change'] = R_1M_data.adjust_price[i]/R_1M_data.adjust_price[i+1] - 1
            R_1M_data.loc[R_1M_data.index[i], 'index_change'] = R_1M_data.index_close[i]/R_1M_data.index_close[i+1] - 1
            # print R_1M_data.stock_date[i]

        # print R_1M_data

        i = len(R_1M_data)
        j = i - 60
        while j >= 0:
            # print R_1M_data.index_change[j:i]
            X = R_1M_data.index_change[j:i]
            Y = R_1M_data.stock_change[j:i]
            bench = sm.add_constant(X)
            model = regression.linear_model.OLS(Y, bench).fit()
            # print R_1M_data.index[j]
            alpha = model.params[0]
            beta = model.params[1]
            R_1M_data.loc[R_1M_data.index[j], 'ALPHA'] = alpha
            R_1M_data.loc[R_1M_data.index[j], 'BETA'] = beta

            Y_hat = X * beta + alpha
            residual = Y - Y_hat
            # print residual

            sigma = np.std(residual)
            R_1M_data.loc[R_1M_data.index[j], 'SIGMA'] = sigma
            R_1M_data.loc[R_1M_data.index[j], 'BTSG'] = sigma * beta
            R_1M_data.loc[R_1M_data.index[j], 'SERDP'] = calculate_SERDP(residual)
            # print R_1M_data.loc[R_1M_data.index[j], 'SERDP']

            # 画图
            # plt.scatter(X, Y, alpha=0.3)  # Plot the raw data
            # plt.plot(X, Y_hat, 'r', alpha=0.9)  # Add the regression line, colored in red
            # plt.xlabel('X Value')
            # plt.ylabel('Y Value')
            # plt.show()

            # 循环
            i -= 1
            j = i - 60

        R_1M_data.to_csv('./month_factors/' + stock_code + '_F_1M.csv', index=False)

ss()
