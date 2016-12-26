# encoding: UTF-8

"""
本文件中实现基础数据的操作，包括更新，初始化等等
"""


class DrEngine:
    def __init__(self, _main_engine):
        # sqlite数据库连接
        self.sqlite = _main_engine.sqliteDB
        # 数据获取
        self.mssql = _main_engine.mssqlDB



