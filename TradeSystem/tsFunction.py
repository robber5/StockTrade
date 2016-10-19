# encoding: UTF-8

from datetime import datetime


def output(content):
    """输出内容"""
    print str(datetime.now()) + "：" + content


def todayDate():
    """获取当前本机电脑时间的日期"""
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)