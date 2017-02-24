# coding=utf-8


import pandas as pd
from WindPy import *
import tushare as ts
import csv
import time
from drDataBase import DrEngine
from datetime import datetime
from string import Template


def get_stock_list():
    """获取最新所有股票列表"""
    # 获取退市列表(set保证唯一性)
    terminated_list = set(ts.get_terminated().set_index('code').index.tolist())

    # 获取暂停上市列表(set保证唯一性)
    suspended_list = set(ts.get_suspended().set_index('code').index.tolist())

    # 获取当前A股全部股票列表(set保证唯一性)
    stock_list = set(ts.get_stock_basics().index.tolist())

    # 将unicode转换为string之后加入到stock_list中
    for s in terminated_list:
        stock_list.add(str(s))

    for s in suspended_list:
        stock_list.add(str(s))

    return stock_list


def get_end_day_str():
    """获取当日时间"""
    # date_str = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    date_str = '2016-06-20'
    return date_str


class BasisDataUpdate(object):
    def __init__(self, _main_engine):
        # 获取数据库操作类
        self.drEngine = DrEngine(_main_engine)
        # 获取stock列表
        self.stock_list = get_stock_list()
        # 设置更新截止日期
        self.end_day_str = get_end_day_str()
        # 初始化股票和期货的上次更新时间
        self.stock_start_day_str = '1900-01-01'
        self.futures_start_day_str = '1900-01-01'
        # 数据库导入模板
        self.futures_import_template = Template(
            "INSERT INTO [dbo].[futures_data] VALUES ($futuresID,$date,$open,$high,$low,$close,$volume,$position)")
        self.stock_import_template = Template(
            "INSERT INTO [dbo].[stock_data] VALUES ($code,$date,$open,$high,$low,$close,$change,$volume,$money,$traded_market_value,$market_value,$turnover,$adjust_price,$report_type,$report_date,$PE_TTM,$PS_TTM,$PC_TTM,$PB,$adjust_price_f)")
        # Template("Hi, $name! $name is learning $language")
        self.financial_import_template = Template(
            "INSERT INTO [dbo].[all_financial_data] ([报告类型],[报告日期],[营业总收入],[总市值],[营业利润],[净利润],[资产总计],[负债合计],[长期负债合计],[其中：优先股]) VALUES ($code,$report_type,$report_date,$markert_value,$gross_profit,$net_income,$total_assets,$total_liabilities,$long_term_liabilities,$preference_shares)")

    def basis_date_update(self):
        """外部调用的主函数"""
        self.stock_start_day_str = self.get_stock_start_day_str()
        self.futures_start_day_str = self.get_futures_start_day_str()
        # self.get_tushare_date()
        # self.get_wind_date()
        self.date_import()

    def get_futures_start_day_str(self):
        """获取期货数据记录的最后记录时间"""
        start_date = self.drEngine.mssql.execquery("select distinct [date] from futures_data ORDER BY date desc")[0][0]
        date_str = datetime.strftime(start_date, '%Y-%m-%d')
        return date_str

    def get_stock_start_day_str(self):
        """获取股票数据记录的最后记录时间"""
        start_date = self.drEngine.mssql.execquery("select distinct [date] from stock_data ORDER BY date desc")[0][0]
        date_str = datetime.strftime(start_date, '%Y-%m-%d')
        return date_str

    @staticmethod
    def get_wind_stock_code(_stock_code):
        """将股票原始code转换为wind查询所需要的股票code"""
        if _stock_code[0:1] == '6':
            wind_stock_code = _stock_code + '.SH'
        else:
            wind_stock_code = _stock_code + '.SZ'
        return wind_stock_code

    def futures_import_str(self, param):
        """根据模板生成期货数据插入语句"""
        instr = self.futures_import_template.substitute(
            futuresID=param[0],
            date=param[1],
            open=param[2],
            high=param[3],
            low=param[4],
            close=param[5],
            volume=param[6],
            position=param[7])
        return instr

    def stock_import_str(self, param):
        """根据模板生成行情数据插入语句"""
        instr = self.stock_import_template.substitute(
            code=param[0],
            date=param[1],
            open=param[2],
            high=param[3],
            low=param[4],
            close=param[5],
            change=param[6],
            volume=param[7],
            money=param[8],
            traded_market_value=param[9],
            market_value=param[10],
            turnover=param[11],
            adjust_price=param[12],
            report_type=param[13],
            report_date=param[14],
            PE_TTM=param[15],
            PS_TTM=param[16],
            PC_TTM=param[17],
            PB=param[18],
            adjust_price_f=param[19])
        return instr

    def financial_import_str(self, param):
        """根据模板生成财务数据插入语句"""
        instr = self.financial_import_template.substitute(
            code=param[0],
            report_type=param[1],
            report_date=param[2],
            markert_value=param[3],
            gross_profit=param[4],
            net_income=param[5],
            total_assets=param[6],
            total_liabilities=param[7],
            long_term_liabilities=param[8],
            preference_shares=param[9]
        )
        return instr

    def insert_script(self, str_insert, times=1):
        """mssql数据库插入"""
        for i in range(times):
            self.drEngine.mssql.execute(str_insert)

    # 1 Tushare更新数据下载,保存到csv中待处理
    def get_tushare_date(self):
        """tushare数据获取"""
        for stk in self.stock_list:
            df = ts.get_k_data(code=stk, start=self.stock_start_day_str, end=self.end_day_str, ktype='D', autype='None', index=False,
                               retry_count=10, pause=1)
            df.to_csv('e:/day/' + stk + '.csv', index=False)

    # 2 Wind更新数据读取(财务数据保存在csv中,期货数据直接导入)
    def get_wind_date(self):
        """wind数据获取"""
        # 2.1 行情数据获取（目前仅通过tushare获取）
        # 2.2.2 财务数据获取
        csv_file = file('Wind_Financial.csv', 'wb')
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                'code',
                'date',
                'stm_issuingdate',
                'stm_rpt_s',
                'tot_liab',
                'tot_assets',
                'share_ntrd_prfshare',
                'tot_oper_rev',
                'opprofit',
                'net_profit_is',
                'lt_borrow'])

        w.start()

        if w.isconnected():
            for stk in self.stock_list:
                stock_code = self.get_wind_stock_code(stk)
                data = w.wsd(
                    stock_code,
                    "stm_issuingdate,stm_rpt_s,tot_liab,tot_assets,share_ntrd_prfshare,tot_oper_rev,opprofit,net_profit_is,lt_borrow",
                    self.stock_start_day_str,
                    self.end_day_str,
                    "dataType=1;unit=1;rptType=1;Period=Q")
                i = 0
                for day in data.Times:
                    result = [str(data.Codes[0]),       # code
                              str(data.Times[i]),       # time
                              str(data.Data[0][i]),     # stm_issuingdate
                              str(data.Data[1][i]),     # stm_rpt_s
                              str(data.Data[2][i]),     # tot_liab
                              str(data.Data[3][i]),     # tot_assets
                              str(data.Data[4][i]),     # share_ntrd_prfshare
                              str(data.Data[5][i]),     # tot_oper_rev
                              str(data.Data[6][i]),     # opprofit
                              str(data.Data[7][i]),     # net_profit_is
                              str(data.Data[8][i])]     # lt_borrow
                    i += 1
                    writer.writerow(result)

        w.stop()
        csv_file.close()

        # 2.2.3 期货数据获取,直接写入数据库,注释部分是保存在csv中
        # csv_futures_file = file('Wind_Futures.csv', 'wb')
        # writer = csv.writer(csv_futures_file)
        # writer.writerow(['futuresID', 'date', 'open', 'high', 'low', 'close', 'volume', 'position'])

        w.start()
        if w.isconnected():
            for i in range(4):
                data_IF = w.wsd("IF0" + str(i) + ".CFE", "open,high,low,close,volume,oi", self.futures_start_day_str, self.end_day_str,
                                "TradingCalendar=CFFEX")
                n = 0
                for day in data_IF.Times:
                    result_futures = [str(data_IF.Codes[0]), str(data_IF.Times[n])[0:10], str(data_IF.Data[0][n]),
                                      str(data_IF.Data[1][n]), str(data_IF.Data[2][n]), str(data_IF.Data[3][n]),
                                      str(data_IF.Data[4][n]), str(data_IF.Data[5][n])]
                    n += 1
                    # writer.writerow(result_futures)
                    self.insert_script(self.futures_import_str(result_futures))

                data_IH = w.wsd("IH0" + str(i) + ".CFE", "open,high,low,close,volume,oi", self.futures_start_day_str, self.end_day_str,
                                "TradingCalendar=CFFEX")
                n = 0
                for day in data_IH.Times:
                    result_futures = [str(data_IH.Codes[0]), str(data_IH.Times[n])[0:10], str(data_IF.Data[0][n]),
                                      str(data_IF.Data[1][n]), str(data_IF.Data[2][n]), str(data_IF.Data[3][n]),
                                      str(data_IF.Data[4][n]), str(data_IF.Data[5][n])]
                    n += 1
                    # writer.writerow(result_futures)
                    self.insert_script(self.futures_import_str(result_futures))

                data_IC = w.wsd("IC0" + str(i) + ".CFE", "open,high,low,close,volume,oi", self.futures_start_day_str, self.end_day_str,
                                "TradingCalendar=CFFEX")
                n = 0
                for day in data_IC.Times:
                    result_futures = [str(data_IC.Codes[0]), str(data_IC.Times[n])[0:10], str(data_IF.Data[0][n]),
                                      str(data_IF.Data[1][n]), str(data_IF.Data[2][n]), str(data_IF.Data[3][n]),
                                      str(data_IF.Data[4][n]), str(data_IF.Data[5][n])]
                    n += 1
                    # writer.writerow(result_futures)
                    self.insert_script(self.futures_import_str(result_futures))
        w.stop()
        # csv_futures_file.close()

    # 3 数据汇总写入数据库（按日循环）
    def date_import(self):
        trade_cal = self.drEngine.mssql.execquery(
            "SELECT DISTINCT [date] FROM futures_data where date>'%(select_date)s'"
            % {'select_date': self.stock_start_day_str}
        )

        # 3.1 数据处理
        for day in trade_cal:
            df_Wind_Financial = pd.read_csv('Wind_Financial.csv')
            print(df_Wind_Financial)

            df_Wind_Futures = pd.read_csv("Wind_Futures.csv")
            print(df_Wind_Futures)


"""
列	中文说明	wind对应	备注
报告类型 stm_issuingdate
报告日期 stm_rpt_s
营业总收入 tot_oper_rev
总市值
营业利润 opprofit
净利润 net_profit_is
资产总计 tot_assets
负债合计 tot_liab
长期负债合计
其中：优先股 share_ntrd_prfshare (股)
"""

"""
列	中文说明	wind对应	备注
code	股票的代码	trade_code	需要根据code增加后缀
date	交易日期	datetime
open		open	略有偏差
high		high	略有偏差
low		low	略有偏差
close		close	略有偏差
change	涨跌幅，复权之后的真实涨跌幅	pct_change	略有偏差
volume	成交量	volumn	略有偏差，wind更精确
money	成交额	amt	略有偏差，wind更精确
traded_market_value	流通市值	mkt_cap_float	很大偏差
market_value	总市值	ev	略有偏差
turnover	换手率，成交量/流通股本	turn	略有偏差
adjust_price	后复权价，10位
report_type	最近一期财务报告实际发布的日期	可以通过日期来处理
report_date	最近一期财务报告的类型
PE_TTM	最近12个月市盈率，股价 / 最近12个月归属母公司的每股收益	pe_ttm,val_pe_deducted_ttm	略有偏差
PS_TTM	最近12个月市销率， 股价 / 最近12个月每股营业收入	ps_ttm	略有偏差
PC_TTM	最近12个月市现率， 股价 / 最近12个月每股经营现金流	pcf_ocf_ttm	略有偏差
PB	市净率，股价 / 最近期财报每股净资产	pb_lf	略有偏差
adjust_price_f	前复权价，精确到小数点后10位

wind另还有
交易状态	trade_status
涨跌停状态 maxupordown
"""




