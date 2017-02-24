# coding=utf-8

from WindPy import *
import tushare as ts
import time
from TradeDataRecorder.drDataBase import DrEngine
from string import Template
from TradeSystem.tradeSystemBase.tsFunction import *
import math


def get_stock_list():
    """获取最新所有股票列表"""
    # # 获取退市列表(set保证唯一性)
    # terminated_list = set(ts.get_terminated().set_index('code').index.tolist())
    #
    # # 获取暂停上市列表(set保证唯一性)
    # suspended_list = set(ts.get_suspended().set_index('code').index.tolist())

    # 获取当前A股全部股票列表(set保证唯一性)
    stock_list = set(ts.get_stock_basics().index.tolist())
    # @ todo 保存成csv,读取次数过多之后会报错

    # 将unicode转换为string之后加入到stock_list中
    # for s in terminated_list:
    #     stock_list.add(str(s))
    #
    # for s in suspended_list:
    #     stock_list.add(str(s))

    return stock_list


def get_end_day_str():
    """获取当日时间"""
    # date_str = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    date_str = '2016-06-22'
    return date_str


class BasisDataUpdate(object):
    def __init__(self, _main_engine):
        # wind数据下载量计数器
        self.data_count = 0
        # 获取数据库操作类
        self.drEngine = DrEngine(_main_engine)
        # 获取指数列表
        self.index_list = ['000001.SH', '399001.SZ', '000016.SH', '000300.SH', '000905.SH']
        # 获取stock列表
        self.stock_list = get_stock_list()
        # self.stock_list = ['002111']
        # 设置更新截止日期
        self.end_day_str = get_end_day_str()
        # report_type的列表
        self.report_type_list = ['03-31', '06-30', '09-30', '12-31']
        # 交易日列表
        self.trade_cal = []
        # 初始化股票和期货的上次更新时间
        self.futures_start_day_str = '1900-01-01'
        # 默认开始时间
        self.default_start_day_str = '2016-06-17'
        # 数据库导入模板
        self.index_import_template = Template(
                "INSERT INTO [dbo].[index_data] VALUES ($index_code,$date,$open,$close,$low,$high,$volume,$money,$change)")
        self.futures_import_template = Template(
                "INSERT INTO [dbo].[futures_data] VALUES ($futuresID,$date,$open,$high,$low,$close,$volume,$position)")
        self.stock_import_template = Template(
                "INSERT INTO [dbo].[stock_data] (code,[date],[open],high,low,[close],change,volume,money,traded_market_value,market_value,turnover,PE_TTM,PB,adjust_price,report_type,report_date) VALUES ($code,$date,$open,$high,$low,$close,$change,$volume,$money,$traded_market_value,$market_value,$turnover,$PE_TTM,$PB,$adjust_price,$report_type,$report_date)")
        self.financial_import_template = Template(
                "INSERT INTO [dbo].[all_financial_data] ([股票代码],[报告类型],[报告日期],[营业总收入],[总市值],[营业利润],[净利润],[资产总计],[负债合计],[长期负债合计],[其中：优先股]) VALUES ($code,$report_type,$report_date,$gross_revenue,$markert_value,$gross_profit,$net_income,$total_assets,$total_liabilities,$long_term_liabilities,$preference_shares)")

    def basis_date_update(self):
        """外部调用的主函数"""
        self.futures_start_day_str = self.get_futures_start_day_str()
        # self.get_tushare_date()
        self.get_wind_date()

    def get_futures_start_day_str(self):
        """获取期货数据记录的最后记录时间,获取了期货数据之后"""
        start_date = self.drEngine.mssql.execquery("select distinct [date] from futures_data ORDER BY date desc")[0][0]
        date_str = datetime.strftime(start_date, '%Y-%m-%d')
        return date_str

    def get_stock_start_day_str(self, stk):
        """获取股票数据记录的开始更新时间
        :param stk: 股票代码
        """
        # 如果股票数据已经更新到和期货数据最新的时间节点一致的时候,向后推一个工作日
        result = self.drEngine.mssql.execquery("select distinct [date] from stock_data where code = '" + stk + "' ORDER BY date desc")

        if result:
            start_date = result[0]
            index_num = self.trade_cal.index(start_date)
            if index_num + 1 >= len(self.trade_cal):
                date_str = datetime.strftime(self.trade_cal[index_num][0] + timedelta(days=1), '%Y-%m-%d')
            else:
                date_str = datetime.strftime(self.trade_cal[index_num + 1][0], '%Y-%m-%d')
            if self.default_start_day_str > date_str:
                date_str = self.default_start_day_str
        else:
            date_str = self.default_start_day_str

        return date_str

    def get_index_start_day_str(self):
        """获取指数数据记录的开始更新时间"""
        # 如果指数数据已经更新到和期货数据最新的时间节点一致的时候,向后推一个工作日
        start_date = self.drEngine.mssql.execquery("select distinct [date] from index_data ORDER BY date desc")[0]
        index_num = self.trade_cal.index(start_date)
        if index_num + 1 >= len(self.trade_cal):
            date_str = datetime.strftime(self.trade_cal[index_num][0] + timedelta(days=1), '%Y-%m-%d')
        else:
            date_str = datetime.strftime(self.trade_cal[index_num + 1][0], '%Y-%m-%d')
        return date_str

    def get_trade_cal(self):
        """通过期货数据获取交易日列表"""
        trade_cal = self.drEngine.mssql.execquery("SELECT DISTINCT [date] FROM futures_data order by [date]")
        return trade_cal

    @staticmethod
    def get_wind_stock_code(_stock_code):
        """将股票原始code转换为wind查询所需要的股票code
        :param _stock_code: 原始代码
        """
        if _stock_code[0:1] == '6':
            wind_stock_code = _stock_code + '.SH'
        else:
            wind_stock_code = _stock_code + '.SZ'
        return wind_stock_code

    def index_import_str(self, param):
        """根据模板生成期货数据插入语句
        :param param: 参数集
        """
        instr = self.index_import_template.substitute(
                index_code="'" + param[0] + "'",
                date="'" + param[1] + "'",
                open=param[2],
                close=param[3],
                low=param[4],
                high=param[5],
                volume=param[6],
                money=param[7],
                change=param[8])
        return instr

    def futures_import_str(self, param):
        """根据模板生成期货数据插入语句
        :param param: 参数集
        """
        instr = self.futures_import_template.substitute(
                futuresID="'" + param[0] + "'",
                date="'" + param[1] + "'",
                open=param[2],
                high=param[3],
                low=param[4],
                close=param[5],
                volume=param[6],
                position=param[7])
        return instr

    def stock_import_str(self, param):
        """根据模板生成行情数据插入语句
        :param param: 参数集
        """
        instr = self.stock_import_template.substitute(
                code="'" + param[0] + "'",
                date="'" + param[1] + "'",
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
                PE_TTM=param[12],
                PB=param[13],
                adjust_price=param[14],
                report_type="'" + param[15] + "'" if param[15] != 'null' else param[15],
                report_date="'" + param[16] + "'" if param[16] != 'null' else param[16])
        return instr

    def financial_import_str(self, param):
        """根据模板生成财务数据插入语句
        :param param: 参数集
        """
        instr = self.financial_import_template.substitute(
                code="'" + param[0] + "'",
                report_type="'" + param[1] + "'",
                report_date="'" + param[2] + "'",
                gross_revenue=param[3],
                markert_value=param[4],
                gross_profit=param[5],
                net_income=param[6],
                total_assets=param[7],
                total_liabilities=param[8],
                long_term_liabilities=param[9],
                preference_shares=param[10]
        )
        return instr

    def insert_script(self, str_insert, times=1):
        """mssql数据库插入"""
        for i in range(times):
            self.drEngine.mssql.execnonquery(str_insert)

    # 1 Tushare更新数据下载,保存到csv中待处理
    def get_tushare_date(self):
        """tushare数据获,用于核对数据"""
        for stk in self.stock_list:
            # @ todo 需要修改
            df = ts.get_k_data(code=stk, start=self.futures_start_day_str, end=self.end_day_str, ktype='D', autype='None',
                               index=False,
                               retry_count=10, pause=1)
            df.to_csv('/day/' + stk + '.csv', index=False)

    def get_futures_data(self):
        """期货数据获取,直接写入数据库"""
        if self.futures_start_day_str >= self.end_day_str:
            print('start_date晚于end_date,不需要更新')
            return
        w.start()
        if w.isconnected():
            for i in range(4):
                data_IF = w.wsd("IF0" + str(i) + ".CFE", "open,high,low,close,volume,oi", self.futures_start_day_str,
                                self.end_day_str,
                                "TradingCalendar=CFFEX")

                data_IH = w.wsd("IH0" + str(i) + ".CFE", "open,high,low,close,volume,oi", self.futures_start_day_str,
                                self.end_day_str,
                                "TradingCalendar=CFFEX")

                data_IC = w.wsd("IC0" + str(i) + ".CFE", "open,high,low,close,volume,oi", self.futures_start_day_str,
                                self.end_day_str,
                                "TradingCalendar=CFFEX")

                self.data_count += len(data_IC.Times) * 7 * 3

                n = 0

                if len(data_IF.Times) == len(data_IH.Times) == len(data_IC.Times):
                    for day in data_IF.Times:
                        result_futures_IF = [str(data_IF.Codes[0][0:4]), str(data_IF.Times[n])[0:10], str(data_IF.Data[0][n]),
                                             str(data_IF.Data[1][n]), str(data_IF.Data[2][n]), str(data_IF.Data[3][n]),
                                             str(data_IF.Data[4][n]), str(data_IF.Data[5][n])]
                        result_futures_IH = [str(data_IH.Codes[0][0:4]), str(data_IH.Times[n])[0:10], str(data_IF.Data[0][n]),
                                             str(data_IF.Data[1][n]), str(data_IF.Data[2][n]), str(data_IF.Data[3][n]),
                                             str(data_IF.Data[4][n]), str(data_IF.Data[5][n])]
                        result_futures_IC = [str(data_IC.Codes[0][0:4]), str(data_IC.Times[n])[0:10], str(data_IF.Data[0][n]),
                                             str(data_IF.Data[1][n]), str(data_IF.Data[2][n]), str(data_IF.Data[3][n]),
                                             str(data_IF.Data[4][n]), str(data_IF.Data[5][n])]
                        if n > 0:
                            # 数据写入
                            self.insert_script(self.futures_import_str(result_futures_IF))
                            self.insert_script(self.futures_import_str(result_futures_IH))
                            self.insert_script(self.futures_import_str(result_futures_IC))
                        n += 1
        w.stop()
        print('期货数据更新完毕!')

    def get_index_data(self):
        """指数数据获取,直接写入数据库"""
        w.start()
        if w.isconnected():
            start_day = self.get_index_start_day_str()
            if start_day <= self.end_day_str:
                for index_code in self.index_list:
                    data_index = w.wsd(index_code, "open,close,low,high,volume,amt,pct_chg", start_day,
                                       self.end_day_str, "")

                    self.data_count += len(data_index.Times) * 8

                    n = 0
                    for day in data_index.Times:
                        result_index = [str(data_index.Codes[0][-2:].lower() + data_index.Codes[0][:6]), str(data_index.Times[n])[0:10],
                                        str(data_index.Data[0][n]), str(data_index.Data[1][n]),
                                        str(data_index.Data[2][n]), str(data_index.Data[3][n]),
                                        str(data_index.Data[4][n]), str(data_index.Data[5][n]),
                                        str(data_index.Data[6][n] / 100)]
                        # 数据写入
                        self.insert_script(self.index_import_str(result_index))
                        n += 1
            else:
                print('该日期内指数数据应该更新过!')
        w.stop()
        print('指数数据更新完毕!')

    def get_all_stock_data(self):
        """股票行情数据及财务数据获取"""
        w.start()

        if w.isconnected():
            for stk in self.stock_list:
                print(stk + "进入读取")
                sql_stock_code = ("sh" if stk[0] == '6' else "sz") + stk
                start_day = self.get_stock_start_day_str(sql_stock_code)
                if start_day <= self.end_day_str:
                    stock_code = self.get_wind_stock_code(stk)

                    data_stock = w.wsd(
                            stock_code,
                            "open,high,low,close,pct_chg,volume,amt,mkt_cap_float,ev,turn,pe_ttm,pb_lf",
                            start_day,
                            self.end_day_str,
                            "unit=1;currencyType=")

                    self.data_count += len(data_stock.Times) * 13

                    data_stock_adjust = w.wsd(
                            stock_code,
                            "close",
                            start_day,
                            self.end_day_str,
                            "PriceAdj=B")

                    self.data_count += len(data_stock_adjust.Times) * 2

                    i = 0

                    if len(data_stock.Times) == len(data_stock_adjust.Times):
                        for day in data_stock.Times:
                            print(stk + ":" + str(day))
                            if data_stock.Data[0][i] is not None:
                                query_str = "select top 1 [报告类型],[报告日期] from [dbo].[all_financial_data] where [股票代码] = '" + sql_stock_code + "' order by [报告类型] desc"
                                result = self.drEngine.mssql.execquery(query_str)

                                if result:
                                    report_type = datetime.strftime(result[0][0], '%Y-%m-%d')
                                    report_date = "null" if result[0][1] is None else datetime.strftime(result[0][1], '%Y-%m-%d')
                                    query_report_type_str = report_type[:5] + self.report_type_list[self.report_type_list.index(report_type[-5:]) + 1]
                                else:
                                    report_type = "null"
                                    report_date = "null"
                                    query_report_type_str = str(day)[:5] + self.report_type_list[int(math.ceil((day.month - 0.1) / 3.0) - 2)]

                                data_financial = None

                                if query_report_type_str <= get_format_date_str(datetime.now()):
                                    data_financial = w.wsd(
                                        stock_code,
                                        "stm_issuingdate,tot_oper_rev,opprofit,net_profit_is,tot_assets,tot_liab,lt_borrow,lt_payable,bonds_payable,lt_empl_ben_payable,specific_item_payable,share_ntrd_prfshare",
                                        query_report_type_str,
                                        query_report_type_str,
                                        "dataType=1;unit=1;rptType=1;Days=Alldays")

                                    self.data_count += len(data_financial.Times) * 13

                                if data_financial != 0:
                                    if data_financial.Data[1][0] is None:
                                        data_financial.Data[1][0] = 0.0
                                    if not math.isnan(data_financial.Data[1][0]):
                                        lt_borrow = 0 if data_financial.Data[6][0] is None else data_financial.Data[6][0]
                                        lt_payable = 0 if data_financial.Data[7][0] is None else data_financial.Data[7][0]
                                        bonds_payable = 0 if data_financial.Data[8][0] is None else data_financial.Data[8][0]
                                        lt_empl_ben_payable = 0 if data_financial.Data[9][0] is None else data_financial.Data[9][0]
                                        specific_item_payable = 0 if data_financial.Data[10][0] is None else data_financial.Data[10][0]

                                        result = [
                                            str(data_financial.Codes[0][-2:].lower() + data_financial.Codes[0][:6]),  # code
                                            str(data_financial.Times[0])[0:10],  # 报告类型
                                            str(data_financial.Data[0][0])[0:10],  # stm_rpt_s 报告日期
                                            str(data_financial.Data[1][0]),  # tot_oper_rev 营业总收入
                                            str(data_stock.Data[8][i]),  # ev 总市值先设为0,根据行情来变
                                            str(0 if data_financial.Data[2][0] is None else data_financial.Data[2][0]),  # opprofit 营业利润
                                            str(0 if data_financial.Data[3][0] is None else data_financial.Data[3][0]),  # net_profit_is 净利润
                                            str(0 if data_financial.Data[4][0] is None else data_financial.Data[4][0]),  # tot_assets 资产总计
                                            str(0 if data_financial.Data[5][0] is None else data_financial.Data[5][0]),  # tot_liab 负债合计
                                            str(lt_borrow + lt_payable + bonds_payable + lt_empl_ben_payable + specific_item_payable),
                                            # lt_borrow + lt_payable + bonds_payable + lt_empl_ben_payable + specific_item_payable 长期负债合计
                                            str(0 if data_financial.Data[11][0] is None else data_financial.Data[11][0])]  # share_ntrd_prfshare 其中：优先股

                                        # 判断当前日期为报告日期时才将财务数据写入
                                        if not isinstance(data_financial.Data[0][0], float):
                                            if day >= data_financial.Data[0][0]:
                                                self.insert_script(self.financial_import_str(result))
                                                # 股票行情中的report_date\report_type也对应开始发生变化
                                                report_type = str(data_financial.Times[0])[0:10]
                                                report_date = str(data_financial.Data[0][0])[0:10]

                                result_stock = [
                                    str(data_stock.Codes[0][-2:].lower() + data_stock.Codes[0][:6]),  # code
                                    str(data_stock.Times[i])[0:10],  # date
                                    str(data_stock.Data[0][i]),   # open
                                    str(data_stock.Data[1][i]),   # high
                                    str(data_stock.Data[2][i]),   # low
                                    str(data_stock.Data[3][i]),   # close
                                    str(data_stock.Data[4][i] / 100),   # pct_chg
                                    str(data_stock.Data[5][i]),   # volume
                                    str(data_stock.Data[6][i]),   # amt
                                    str(data_stock.Data[7][i]),   # mkt_cap_float
                                    str(data_stock.Data[8][i]),   # ev
                                    str(data_stock.Data[9][i] / 100),   # turn
                                    str(data_stock.Data[10][i]),   # pe_ttm
                                    str(data_stock.Data[11][i]),   # pb_lf
                                    str(data_stock_adjust.Data[0][i]),  # adjust_close
                                    report_type,
                                    report_date
                                ]
                                # 行情数据写入
                                if result_stock[2] != 'nan' and result_stock[7] != "0.0":             # 根据open判断
                                    params = self.stock_import_str(result_stock)
                                    self.insert_script(params)
                                i += 1
                else:
                    print("该日期内该股票行情及财务数据已经更新过!")
        w.stop()
        print('股票数据更新完毕!')

    # 2 Wind更新数据读取(财务数据保存在csv中,期货数据直接导入)
    def get_wind_date(self):
        """wind数据获取"""
        self.get_futures_data()   # 读取期货数据
        self.trade_cal = self.get_trade_cal()   # 读取交易日历
        self.get_index_data()  # 读取交易日历
        self.get_all_stock_data()  # 读取股票相关数据
        print("更新完毕! 使用数据量:" + str(self.data_count))



        #字段说明
        """
        列	中文说明	wind对应	备注
        报告类型 stm_issuingdate
        报告日期 stm_rpt_s
        营业总收入 tot_oper_rev
        总市值 market_value
        营业利润 opprofit
        净利润 net_profit_is
        资产总计 tot_assets
        负债合计 tot_liab
        长期负债合计 lt_borrow+lt_payable+bonds_payable+lt_empl_ben_payable+specific_item_payable
        其中：优先股 share_ntrd_prfshare (股)
        """

        """
        列	中文说明	wind对应	备注
        code	股票的代码	trade_code
        date	交易日期	datetime
        open		open
        high		high
        low		low
        close		close
        change	涨跌幅，复权之后的真实涨跌幅	pct_change 差100倍
        volume	成交量	volume
        money	成交额	amt
        traded_market_value 流通市值 mkt_cap_float
        market_value	总市值	ev
        turnover	换手率，成交量/流通股本	turn 差100倍
        PE_TTM	最近12个月市盈率，股价 / 最近12个月归属母公司的每股收益	pe_ttm
        PB	市净率，股价 / 最近期财报每股净资产	pb_lf
        adjust_price	后复权价，10位
        report_type	最近一期财务报告实际发布的日期	可以通过日期来处理
        report_date	最近一期财务报告的类型  可以通过日期来处理
        """

