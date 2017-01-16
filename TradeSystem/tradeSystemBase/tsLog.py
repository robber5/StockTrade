# encoding: UTF-8

"""
***example***

from tradeSystemBase.tsLog import *

log = Log(__file__.split('/')[-1])
try:
    log.logger.info("msg")
    if True:
        pass
    else:
        raise CustomError("msg")
except BaseException as msg:
    log.logger.exception(msg)
"""

import logging
import time
import logging.handlers

rq = time.strftime('%Y%m%d', time.localtime(time.time()))


class Log(object):
    """日志类"""
    def __init__(self, _name):
        self.path = "/log/"  # 定义日志存放路径
        self.filename = self.path + rq + '.log'  # 日志文件名称
        self.name = _name    # 为%(name)s赋值
        self.logger = logging.getLogger(self.name)
        # 控制日志文件中记录级别
        self.logger.setLevel(logging.INFO)
        # 控制输出到控制台日志格式、级别
        self.ch = logging.StreamHandler()
        gs = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s[line:%(lineno)d] - %(message)s')
        self.ch.setFormatter(gs)
        # 日志保留10天,一天保存一个文件
        self.fh = logging.handlers.TimedRotatingFileHandler(self.filename, 'D', 1, 10)
        # 定义日志文件中格式
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s -   %(name)s[line:%(lineno)d] - %(message)s')
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)


class CustomError(Exception):
    """
    自定义异常类,用在主动输出异常时使用,用 raise关键字配合使用,例:
          if True:
                pass
          else:
                raise customError(msg)
    """
    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        if self.msg:
            return self.msg
        else:
            return u"输入不符合规则!"