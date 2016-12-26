# encoding: UTF-8

from datetime import datetime


def output(content):
    """输出内容"""
    print "INFO(" + str(datetime.now()) + ")::" + str(content)


def get_today_date():
    """获取当前本机电脑时间的日期"""
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def is_num(value):
    try:
        value + 1
    except TypeError:
        return False
    else:
        return True


def get_format_date(date):
    return datetime.strftime(date, '%Y-%m-%d')
