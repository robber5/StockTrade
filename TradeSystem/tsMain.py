# coding=utf-8

# 引入需要的模块

import json
from tsEngine import MainEngine


def main():
    """主程序入口"""
    # 读取配置文件
    try:
        f = file("Trade_Setting.json")
        setting = json.load(f)
        print(setting['StrategyTitle'])
    except Exception, ex:
        print('配置文件载入错误：'+ex)

    # 载入配置到交易模块
    MainEngine
    (
        setting['MS_host'],
        setting['MS_user'],
        setting['MS_pwd'],
        setting['MS_db'],
        setting['sqlite_DB']
    ).start()

if __name__ == '__main__':
    main()

