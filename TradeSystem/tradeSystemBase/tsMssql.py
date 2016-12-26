# coding=utf-8

import pymssql
import pandas as pd
from datetime import *


class MSSQL:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __getconnect(self):
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

    def execquery(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__getconnect()
        cur.execute(sql)
        reslist = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return reslist

    def execqueryparam(self, sql, param):
        cur = self.__getconnect()
        cur.execute(sql, param)
        reslist = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return reslist

    def execnonquery(self, sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__getconnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def get_open_price(self, current_date, _operate_list):
        """获取当日开盘价(前复权)"""
        select_date = datetime.strftime(current_date, '%Y-%m-%d')

        select_str = ""

        for stk in _operate_list:
            select_str = select_str + '\'' + stk + '\','

        select_str = select_str[:-1]

        sql_tuple = self.execquery("SELECT code,[open]*adjust_price_f/[close] AS open_price,high*adjust_price_f/[close] as high_price,low*adjust_price_f/[close] as low_price  FROM stock_data WHERE date = '" + select_date + "' AND code IN (" + select_str + ")")

        df = pd.DataFrame(sql_tuple, columns=('stockcode', 'open', 'high', 'low'))

        return df

    def get_stock_close(self, df, current_date):
        """获取股票池收盘价(前复权)"""
        select_date = datetime.strftime(current_date, '%Y-%m-%d')

        query_line = "SELECT code,adjust_price_f FROM stock_data WHERE date = '" + select_date + "'"

        sql_tuple = self.execquery(query_line)

        history = pd.DataFrame(sql_tuple, columns=['code', 'adjust_price_f'])

        # 停牌股票选择上一个交易日
        suspension = list(set(df.index.values).difference(set(history['code'].values)))

        if len(suspension) != 0:
            select_list = ''

            for stk in suspension:
                select_list = select_list + '\'' + stk + '\','

            select_list = select_list[:-1]

            query_line = "WITH rank_stock(code, adjust_price_f ,rank_rn) AS (SELECT code, adjust_price_f ,DENSE_RANK() OVER(PARTITION BY code ORDER BY [date] desc) FROM stock_data where date<'" + select_date +"' and code in(" + select_list + ")) SELECT code,adjust_price_f FROM rank_stock WHERE rank_rn = 1"

            sql_tuple = self.execquery(query_line)
            df = pd.DataFrame(sql_tuple, columns=('code', 'adjust_price_f'))
            history = pd.concat([history, df])

        return history

    def get_close_price(self, current_date):
        """获取股票池收盘价(前复权)"""
        select_date = datetime.strftime(current_date, '%Y-%m-%d')

        query_line = "SELECT code,adjust_price_f FROM stock_data WHERE date = '" + select_date + "'"

        sql_tuple = self.execquery(query_line)

        history = pd.DataFrame(sql_tuple, columns=['code', 'adjust_price_f'])

        return history

    def get_index(self, benchmark, current_date, column):
        """获取指数收盘价(前复权)"""
        select_date = datetime.strftime(current_date, '%Y-%m-%d')
        index_value = self.execquery(
            "SELECT [" + column + "] FROM index_data WHERE index_code = '" + benchmark + \
            "' AND date = '" + select_date + "'")
        return index_value[0][0]

    def get_stock_pool(self, key):
        """获取股票池"""
        stockpool = []
        if key == 'A':
            sql_tuple = self.execquery("SELECT DISTINCT [code] FROM stock_data where adjust_price_f is NOT NULL ")
            df = pd.DataFrame(sql_tuple)
            stockpool = df[0].tolist()

        # if key == 'ZZ500':
        #     sql_tuple = self.execquery("")
        #     df = pd.DataFrame(sql_tuple)
        #     stockpool = df[0].tolist()
        return stockpool

    def get_history(self, column_list, current_date):
        select_date = datetime.strftime(current_date, '%Y-%m-%d')
        sql_tuple = self.execquery("SELECT code," + column_list + " FROM stock_data where date ='" + select_date + "'")
        df = pd.DataFrame(sql_tuple)
        return df

    def get_index_open(self, benchmark, current_date):
        """获取指数收盘价(前复权)"""
        select_date = datetime.strftime(current_date, '%Y-%m-%d')
        index_value = self.execquery(
            "SELECT [close] AS indexclose FROM index_data WHERE index_code = '" + benchmark +
            "' AND date = '" + select_date + "'")
        return index_value[0][0]

