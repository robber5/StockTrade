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

@profile
def ss():

    # 查找已有文件
    dir = './week_factors'
    files = os.listdir(dir)
    file_list = []
    for name in files:
        stock_code = name[0:8]
        file_list.append(stock_code)

    print file_list

    # 连接数据库
    ms = MSSQL(host="localhost", user="USER-GH61M1ULCU\Administrator", pwd="12qwasZX", db="stocks")




    # 读取股票列表
    SQL_tuple = ms.ExecQuery("SELECT DISTINCT [股票代码] FROM [stocks].[dbo].[all_financial_data] ")
    stock_list = pd.DataFrame(SQL_tuple, columns=['stock_code'])

    # 循环计算
    for stock_code in stock_list.stock_code:

        if stock_code in file_list:  # 如果已经有结果文件. 继续
            continue

        SQL_tuple = ms.ExecQueryPara("select * from [dbo].[V_stock_1w] where code=%s order by date DESC", stock_code)
        Stock_Volm_data = pd.DataFrame(SQL_tuple, columns=['code', 'dateS', 'adjust_price', 'moneyS'])
        Stock_Volm_data.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
        for i in range(0, len(Stock_Volm_data)-1):
            Stock_Volm_data.loc[Stock_Volm_data.index[i], 'stock_change'] = Stock_Volm_data.adjust_price[i]/Stock_Volm_data.adjust_price[i+1] - 1

        # print Stock_Volm_data

        if stock_code[0:2] == 'sh':
            index_code = 'sh000001'
        elif stock_code[0:2] == 'sz':
            index_code = 'sz399001'

        print stock_code + '---< ' + index_code

        SQL_tuple = ms.ExecQueryPara("select * from [dbo].[V_index_1w] where index_code=%s order by date DESC", index_code)
        Index_Volm_data = pd.DataFrame(SQL_tuple, columns=['index_code', 'dateI', 'close', 'moneyI'])
        Index_Volm_data.set_index('dateI', drop=False, inplace=True)  # dateI 列设为index
        for i in range(0, len(Index_Volm_data)-1):
            Index_Volm_data.loc[Index_Volm_data.index[i], 'index_change'] = Index_Volm_data.close[i]/Index_Volm_data.close[i+1] - 1

        # print Index_Volm_data

        Index_Volm_dataVSstock = Index_Volm_data.reindex(list(Stock_Volm_data.index), ['dateI', 'moneyI', 'close', 'index_change'])

        # print Index_Volm_dataVSstock

        for i in range(len(Stock_Volm_data) - 2, -1, -1):
            # for i in range(0, 5):
            dateNow = Stock_Volm_data.dateS[i]
            dateNowStr = str(dateNow.date())
            if dateNow != Index_Volm_dataVSstock.dateI[i]:
                tmpVolm = Index_Volm_data[Index_Volm_data.dateI > dateNow].tail(1)
                Index_Volm_dataVSstock.loc[dateNowStr, 'dateI'] = tmpVolm['dateI'][0]
                Index_Volm_dataVSstock.loc[dateNowStr, 'moneyI'] = tmpVolm['moneyI'][0]
                Index_Volm_dataVSstock.loc[dateNowStr, 'close'] = tmpVolm['close'][0]
                Index_Volm_dataVSstock.loc[dateNowStr, 'index_change'] = tmpVolm['index_change'][0]
                # print 'e------------------------------------------e'
                # print Index_Volm_dataVSstock.loc[str(dateNow.date())]

            Index_Volm_dataVSstock.loc[dateNowStr, 'V_changeI'] = Index_Volm_dataVSstock.moneyI[i] / Index_Volm_dataVSstock.moneyI[i + 1] - 1
            Stock_Volm_data.loc[dateNowStr, 'V_changeS'] = Stock_Volm_data.moneyS[i] / Stock_Volm_data.moneyS[i + 1] - 1
            Stock_Volm_data.loc[dateNowStr, 'dateI'] = Index_Volm_dataVSstock.dateI[dateNowStr]
            Stock_Volm_data.loc[dateNowStr, 'closeI'] = Index_Volm_dataVSstock.close[dateNowStr]
            Stock_Volm_data.loc[dateNowStr, 'index_change'] = Index_Volm_dataVSstock.index_change[dateNowStr]
            if np.isnan(Index_Volm_dataVSstock.moneyI[i] / Index_Volm_dataVSstock.moneyI[i + 1] - 1):
                print 'Index-1W-nan----------------------------------------' + dateNowStr
                print Index_Volm_dataVSstock.moneyI[i]
                print Index_Volm_dataVSstock.moneyI[i + 1]
            if np.isnan(Stock_Volm_data.moneyS[i] / Stock_Volm_data.moneyS[i + 1] - 1):
                print 'Stock-1W-nan----------------------------------------' + dateNowStr
                print Stock_Volm_data.moneyS[i]
                print Stock_Volm_data.moneyS[i + 1]

        # print Index_Volm_dataVSstock

        i = len(Stock_Volm_data) - 1
        j = i - 60
        while j >= 0:
            X = Index_Volm_dataVSstock.V_changeI[j:i]
            Y = Stock_Volm_data.V_changeS[j:i]
            # print X
            # print Y
            bench = sm.add_constant(X)
            model = regression.linear_model.OLS(Y, bench).fit()
            # print R_1M_data.index[j]
            alpha = model.params[0]
            beta = model.params[1]
            Stock_Volm_data.loc[Stock_Volm_data.index[j], 'VOLBT'] = beta
            # print beta

            # 画图
            # Y_hat = X * beta + alpha
            # plt.scatter(X, Y, alpha=0.3)  # Plot the raw data
            # plt.plot(X, Y_hat, 'r', alpha=0.9)  # Add the regression line, colored in red
            # plt.xlabel('X Value')
            # plt.ylabel('Y Value')
            # plt.show()

            # 循环
            i -= 1
            j = i - 60

        # print Stock_Volm_data.VOLBT
        Stock_Volm_data.to_csv( './week_factors/' + stock_code + '_F_1W.csv', index=False)

ss()
