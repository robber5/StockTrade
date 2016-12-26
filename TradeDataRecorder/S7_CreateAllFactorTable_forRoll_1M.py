# coding=utf-8

import sqlite3
import pymssql
import pandas as pd
import datetime

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
        """

        :rtype: object
        """
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



# 连接数据库
ms = MSSQL(host="localhost", user="USER-GH61M1ULCU\Administrator", pwd="12qwasZX", db="stocks")

# 读取股票列表
SQL_tuple = ms.ExecQuery("SELECT DISTINCT [股票代码] FROM [stocks].[dbo].[all_financial_data] ")
stock_list = pd.DataFrame(SQL_tuple, columns=['stock_code'])

conn = sqlite3.connect('E:/Wanda_Work/sqlite/stock.db')
SQL_tuple = pd.read_sql_query("SELECT DISTINCT [code] FROM stock_all_1M_all_factors_for_roll", conn)
file_list = pd.DataFrame(SQL_tuple, columns=['code'])
print list(file_list.code)

# 循环计算
for stock_code in stock_list.stock_code:
# for stock_code in ['sh600000']:
    if stock_code in list(file_list.code):  # 如果已经有该股票结果. 继续
        continue

    print stock_code + '  -----------------------------------------------//'
    # 读取周factors
    SQL_tuple = ms.ExecQueryPara("select [code],[dateS],[dateI],[VOLBT],[stock_change],[index_change]  \
    from [dbo].[week_factors] where code=%s order by dateS DESC", stock_code)
    Wdata = pd.DataFrame(SQL_tuple, columns=['code', 'dateS', 'dateI', 'VOLBT', 'stock_change', 'index_change'])
    Wdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
    # print Wdata

    # 读取日factors
    SQL_tuple = ms.ExecQueryPara("select [code],[date],[report_type],[ETOP],[STO_1M],[STO_3M],[STO_6M],[STO_12M],[STO_60M]  \
      ,[RSTR_1M],[RSTR_3M],[RSTR_6M],[RSTR_12M],[LNCAP],[BTOP],[STOP],[HILO],[DASTD],[LPRI],[CMRA]   \
       from [dbo].[day_factors] where code=%s order by date DESC", stock_code)
    Ddata = pd.DataFrame(SQL_tuple, columns=['code', 'date', 'report_type', 'ETOP', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M'\
                                             ,'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'LNCAP', 'BTOP', 'STOP', 'HILO', 'DASTD', 'LPRI', 'CMRA'])
    Ddata.set_index('date', drop=False, inplace=True)  # date 列设为index
    # print Ddata

     # 读取月factors
    SQL_tuple = ms.ExecQueryPara("select [code],[stock_trade_date],[index_trade_dateI],[ALPHA],[BETA],[SIGMA],[BTSG],[SERDP],[stock_change],[index_change]  \
    from [dbo].[month_factors] where code=%s order by stock_trade_date DESC", stock_code)
    Mdata = pd.DataFrame(SQL_tuple, columns=['code', 'dateS', 'dateI', 'ALPHA', 'BETA', 'SIGMA', 'BTSG', 'SERDP', 'stock_change', 'index_change'])
    Mdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
    # print Mdata

    # 读取季factors
    SQL_tuple = ms.ExecQueryPara("select [code],[report_type],[ETP5],[AGRO],[MLEV],[S_GPM],[C_GPM],[T_GPM] \
      ,[S_NPM],[C_NPM],[T_NPM],[S_ROE],[C_ROE],[T_ROE],[S_ROA],[C_ROA],[T_ROA]  \
    from [dbo].[quarter_factors] where code=%s order by report_type DESC", stock_code)
    Qdata = pd.DataFrame(SQL_tuple, columns=['code', 'report_type', 'ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                                             ,'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE' ,'S_ROA', 'C_ROA', 'T_ROA'])
    Qdata.set_index('report_type', drop=False, inplace=True)  # report_type 列设为index
    # print Qdata

    # 主时间轴 设为 "月"
    df = pd.DataFrame(Mdata.loc[:, ['code', 'dateS', 'dateI']], index=Mdata.dateS, columns=['code', 'dateS', 'dateI', 'report_type', 'VOLBT'\
                                                                                            , 'ETOP', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M'\
                                             ,'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'LNCAP', 'BTOP', 'STOP', 'HILO', 'DASTD', 'LPRI', 'CMRA'\
                                                                                            , 'ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA'\
                                                                                            , 'ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                                             ,'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE' ,'S_ROA', 'C_ROA', 'T_ROA'
                                                                                            , 'stock_change', 'index_change'])
    # print df.head(1).T
    # print len(Mdata)
    for i in range(0, len(Mdata)-1):
        # print i
        dateNw  = Mdata.dateS[i]
        # datePre = Mdata.dateS[i+1]
        # print str(dateNw.date()) + '-----' + str(datePre.date())
        rp_typeNw = Ddata.report_type[dateNw]
        # print str(rp_typeNw.date())

        df.loc[dateNw, 'report_type'] = rp_typeNw.date()

        # 周factors 对齐
        tmpVolm = Wdata[Wdata.dateS <= dateNw].head(1)
        if len(tmpVolm)>0:
            df.loc[dateNw, 'VOLBT'] = tmpVolm['VOLBT'][0]
        # 日factors 对齐
        # 与价格无关的因子, 对齐当前因子
        df.loc[dateNw,  ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M']]\
            = Ddata.loc[dateNw, [ 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M']]
        # 与绝对价格有关的因子, 对齐当期, 但不受停牌影响
        df.loc[dateNw,  ['ETOP', 'LNCAP', 'BTOP', 'STOP', 'LPRI']]\
                = Ddata.loc[dateNw, ['ETOP', 'LNCAP', 'BTOP', 'STOP', 'LPRI']]
        # 与收益率相关的因子, 对其当期, 同时受到停牌影响
        df.loc[dateNw,  ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]\
                = Ddata.loc[dateNw, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]

        # 月factors 对齐
        df.loc[dateNw,  ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA', 'stock_change', 'index_change']] \
            = Mdata.loc[dateNw,  ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA', 'stock_change', 'index_change']]
        # 季factors 对齐
        tmpVolm = Qdata[Qdata.report_type == rp_typeNw]
        if len(tmpVolm)>0:
            # print tmpVolm
            df.loc[dateNw,  ['ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                                             ,'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE' ,'S_ROA', 'C_ROA', 'T_ROA']]\
                = tmpVolm.loc[tmpVolm.report_type[0], ['ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                                             ,'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE' ,'S_ROA', 'C_ROA', 'T_ROA']]



    # print df.T
    df.to_sql('stock_all_1M_all_factors_for_roll', conn, if_exists='append', index=False, chunksize=100000)