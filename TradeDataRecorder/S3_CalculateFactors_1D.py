# coding=utf-8
import datetime
import pymssql
import pandas as pd
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


def ss():
    # 查找已有文件
    dir = './day_factors'
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

        print stock_code

        # 获取年报的营业收入数据
        SQL_tuple = ms.ExecQueryPara("SELECT [股票代码], [报告类型], [报告日期], [营业总收入] \
          FROM [stocks].[dbo].[all_financial_data] WHERE 股票代码=%s AND MONTH(报告类型)=12 \
          AND 报告日期 IS NOT NULL ORDER BY 报告类型 DESC", stock_code)
        finance_data = pd.DataFrame(SQL_tuple, columns=['code', 'report_type', 'report_date', 'operating_revenue'])
        finance_data.set_index('report_type', drop=False, inplace=True)  # date列设为index

        # 获取股票日数据
        SQL_tuple = ms.ExecQueryPara("SELECT  [code] \
                                              ,[date] \
                                              ,[high] \
                                              ,[low] \
                                              ,[close] \
                                              ,[change] \
                                              ,[traded_market_value] \
                                              ,[market_value] \
                                              ,[turnover] \
                                              ,[adjust_price] \
                                              ,[report_type] \
                                              ,[report_date] \
                                              ,[PE_TTM] \
                                              ,[adjust_price_f] \
                                              ,[PB] \
          FROM [stocks].[dbo].[stock_data]\
          WHERE code=%s\
          ORDER BY date DESC", stock_code)
        stock_data = pd.DataFrame(SQL_tuple, columns=['code', 'date', 'high', 'low', 'close', 'change', \
                                                      'traded_market_value', 'market_value', 'turnover', \
                                                      'adjust_price', 'report_type', 'report_date', \
                                                      'PE_TTM', 'adjust_price_f', 'PB'])  # 转为DataFrame 并设定列名
        stock_data.set_index('date', drop=False, inplace=True)  # date列设为index
        stock_data['Hi_adj'] = (stock_data['high'] / stock_data['close']) * stock_data['adjust_price']  # 非因子 计算后复权后的high
        stock_data['Lo_adj'] = (stock_data['low'] / stock_data['close']) * stock_data['adjust_price']  # 非因子 计算后复权后的low

        # _1     Earning Yield 因子项 ETOP: 最近12个月净利润/最新市值,即PE的倒数
        stock_data['ETOP'] = 1.0 / stock_data['PE_TTM']
        stock_data['BTOP'] = 1.0 / stock_data['PB']
        # _2
        dateList = list(stock_data.index)
        for i in range(len(dateList)-1, -1, -1):
            dateNw = dateList[i]

            # 计算STOP, 需要最近一期的年报
            rp_typeNw = stock_data.loc[dateNw, 'report_type']
            rp_typeMatch = finance_data[finance_data.report_type <= rp_typeNw]['operating_revenue']
            # print str(rp_typeNw.date())
            # print str(rp_typeMatch.head(1).index)
            if len(rp_typeMatch) > 0:
                operating_revenue = rp_typeMatch.head(1)[0]
                stock_data.loc[dateNw, 'STOP'] = operating_revenue/stock_data['market_value'][i]

            # 计算日波动率
            if len(dateList) - i > 65:
                rList = list(stock_data[i:i+65]['change'])
                # print rList
                stock_data.loc[dateNw, 'DASTD'] = math.sqrt(23.0*sum([c*c for c in rList]))  # 65天收益率的平方和乘以23,开根

            # 计算日变相关factor
            if stock_data['market_value'][i] > 0:
                stock_data.loc[dateNw, 'LNCAP'] = math.log(stock_data['market_value'][i], math.e)
            stock_data.loc[dateNw, 'LPRI'] = math.log(stock_data['close'][i], math.e)

            m1st_date = stock_data[str(dateNw.year) + '-' + str(dateNw.month)]['date'].tail(1)[0]
            # stock_data.loc[dateNw, 'MRA'] = sum(stock_data[dateNw:m1st_date]['change'])  # # 非因子 计算当月累积收益
            stock_data.loc[dateNw, 'MRA'] = (stock_data.loc[dateNw, 'adjust_price']/stock_data.loc[m1st_date, 'adjust_price'])-1.0  # # 非因子 计算当月累积收益
            stock_data.loc[dateNw, 'CMRA'] = math.log(
                (1 + max(stock_data[dateNw:m1st_date]['MRA'])) / (1 + min(stock_data[dateNw:m1st_date]['MRA'])),
                math.e)  # 计算当月累积收益范围 CMRA

            # 需一个月数据的相关factor
            # 一个月的起点(4周)
            date1M = stock_data['date'][i] + datetime.timedelta(days=-(7 * 4))
            if len(stock_data[stock_data.date <= date1M]['date']) == 0:  # 上市不满一个月, 跳过此次循环
                continue
            else:
                date1M = stock_data[stock_data.date >= date1M]['date'].tail(1)[0]
                sttpos1M = dateList.index(date1M)
            # print str(dateNw.weekday())+'__'+str(date1M.weekday())
            # print str(dateNw.date())+'__'+str(date1M.date())

            stock_data.loc[dateNw, 'STO_1M'] = sum(stock_data[i:sttpos1M + 1]['turnover'])
            # stock_data.loc[dateNw, 'RSTR_1M'] = sum(stock_data[i:sttpos1M + 1]['change'])
            stock_data.loc[dateNw, 'RSTR_1M'] = (stock_data.loc[dateNw, 'adjust_price']/stock_data.loc[date1M, 'adjust_price'])-1.0
            stock_data.loc[dateNw, 'HILO'] = math.log(
                max(stock_data[i:sttpos1M + 1]['Hi_adj']) / min(stock_data[i:sttpos1M + 1]['Lo_adj']), math.e)

            # 需三个月数据的相关factor
            # 三个月的起点(13周)
            date3M = stock_data['date'][i] + datetime.timedelta(days=-(7 * 13))
            if len(stock_data[stock_data.date <= date3M]['date']) == 0:  # 上市不满3个月, 跳过此次循环
                continue
            else:
                date3M = stock_data[stock_data.date >= date3M]['date'].tail(1)[0]
                sttpos3M = dateList.index(date3M)
            # print str(dateNw.date())+'__'+str(date3M.date())

            stock_data.loc[dateNw, 'STO_3M'] = sum(stock_data[i:sttpos3M + 1]['turnover'])
            # stock_data.loc[dateNw, 'RSTR_3M'] = sum(stock_data[i:sttpos3M + 1]['change'])
            stock_data.loc[dateNw, 'RSTR_3M'] = (stock_data.loc[dateNw, 'adjust_price']/stock_data.loc[date3M, 'adjust_price'])-1.0

            # 需六个月数据的相关factor
            # 六个月的起点(26周)
            date6M = stock_data['date'][i] + datetime.timedelta(days=-(7 * 26))
            if len(stock_data[stock_data.date <= date6M]['date']) == 0:  # 上市不满6个月, 跳过此次循环
                continue
            else:
                date6M = stock_data[stock_data.date >= date6M]['date'].tail(1)[0]
                sttpos6M = dateList.index(date6M)
            # print str(dateNw.date())+'__'+str(date6M.date())

            stock_data.loc[dateNw, 'STO_6M'] = sum(stock_data[i:sttpos6M + 1]['turnover'])
            # stock_data.loc[dateNw, 'RSTR_6M'] = sum(stock_data[i:sttpos6M + 1]['change'])
            stock_data.loc[dateNw, 'RSTR_6M'] = (stock_data.loc[dateNw, 'adjust_price']/stock_data.loc[date6M, 'adjust_price'])-1.0

            # 需12个月数据的相关factor
            # 12个月的起点(52周)
            date12M = stock_data['date'][i] + datetime.timedelta(days=-(7 * 52))
            if len(stock_data[stock_data.date <= date12M]['date']) == 0:  # 上市不满12个月, 跳过此次循环
                continue
            else:
                date12M = stock_data[stock_data.date >= date12M]['date'].tail(1)[0]
                sttpos12M = dateList.index(date12M)
            # print str(dateNw.date())+'__'+str(date6M.date())

            stock_data.loc[dateNw, 'STO_12M'] = sum(stock_data[i:sttpos12M + 1]['turnover'])
            # stock_data.loc[dateNw, 'RSTR_12M'] = sum(stock_data[i:sttpos12M + 1]['change'])
            stock_data.loc[dateNw, 'RSTR_12M'] = (stock_data.loc[dateNw, 'adjust_price']/stock_data.loc[date12M, 'adjust_price'])-1.0

            # 需60个月数据的相关factor
            # 60个月的起点(260周)
            date60M = stock_data['date'][i] + datetime.timedelta(days=-(7 * 260))
            if len(stock_data[stock_data.date <= date60M]['date']) == 0:  # 上市不满5年, 跳过此次循环
                continue
            else:
                date60M = stock_data[stock_data.date >= date60M]['date'].tail(1)[0]
                sttpos60M = dateList.index(date60M)
            # print str(dateNw.date())+'__'+str(date6M.date())

            stock_data.loc[dateNw, 'STO_60M'] = sum(stock_data[i:sttpos60M + 1]['turnover'])
            # stock_data.loc[dateNw, 'RSTR_60M'] = sum(stock_data[i:sttpos60M + 1]['change'])
            stock_data.loc[dateNw, 'RSTR_60M'] = (stock_data.loc[dateNw, 'adjust_price']/stock_data.loc[date60M, 'adjust_price'])-1.0

        stock_data.to_csv( './day_factors/' + stock_code + '_F_1D.csv', index=False)

ss()
