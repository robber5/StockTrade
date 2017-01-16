# encoding: UTF-8

from collections import OrderedDict
from sqlalchemy import create_engine

from positionManage.tsPositionManage import PositionEngine
from tradeSystemBase.tsMssql import MSSQL


class MainEngine(object):
    """主引擎"""

    def __init__(self, _host, _user, _pwd, _db, _sqlite_path):
        """Constructor"""
        # 创建对冲引擎
        # self.eventEngine = HedgeEngine()
        # self.eventEngine.start()

        # 数据库连接
        self.mssqlDB = MSSQL(host=_host, user=_user, pwd=_pwd, db=_db)
        self.sqliteDB = create_engine('sqlite:///' + _sqlite_path)

        # 调用一个个初始化函数
        self.gatewayDict = None
        self.init_gateway()

        # 扩展模块(行情更新模块DataEngine,风控模块RiskEngine)
        self.positionEngine = PositionEngine()

    def init_gateway(self):
        """初始化接口对象"""
        # 用来保存接口对象的字典
        self.gatewayDict = OrderedDict()
        # 各交易api的接口的调用
