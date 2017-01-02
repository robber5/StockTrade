# coding=utf-8
import time


class Log(object):
    """日志数据类"""
    def __init__(self):
        self.logTime = time.strftime('%X', time.localtime())  # 日志生成时间
        self.logContent = EMPTY_UNICODE  # 日志信息

    def writeLog(self, content):
        """快速发出日志事件"""
        log = VtLogData()
        log.logContent = content
        event = Event(type_=EVENT_LOG)
        event.dict_['data'] = log
        self.eventEngine.put(event)