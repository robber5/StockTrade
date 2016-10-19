# coding=utf-8

import json
from tsEngine import MainEngine
from tsStrategy import StrategyTest


def main():
    """主程序入口"""
    # 读取配置文件
    setting = dict
    try:
        f = file("Trade_Setting.json")
        setting = json.load(f)
        print(setting['StrategyTitle'])
    except Exception, ex:
        print('配置文件载入错误：' + ex.message)

    # 载入配置到交易模块
    main_engine = MainEngine(
        setting['MS_host'],
        setting['MS_user'],
        setting['MS_pwd'],
        setting['MS_db'],
        setting['sqlite_DB'],
        setting['engine_type']
    )

    # 运行
    StrategyTest(main_engine).run()

if __name__ == '__main__':
    main()
