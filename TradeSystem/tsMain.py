# coding=utf-8

import json
from TradeSystem.selectStrategy.tsStrategy import StrategyTest
from tsEngine import MainEngine
from TradeDataRecorder.drDataUpdate import DataUpdate


def main():
    """主程序入口"""
    # 读取配置文件
    setting = dict
    try:
        f = file("Trade_Setting.json")
        setting = json.load(f)
        print(setting['StrategyTitle'].encode('utf-8') + "开始运行")
    except Exception, ex:
        print('配置文件载入错误：' + ex.message)

    # 载入配置到交易模块
    main_engine = MainEngine(
        setting['MS_host'].encode('utf-8'),
        setting['MS_user'].encode('utf-8'),
        setting['MS_pwd'].encode('utf-8'),
        setting['MS_db'].encode('utf-8'),
        setting['sqlite_DB'].encode('utf-8')
    )

    DataUpdate(main_engine).run()

    if setting['engine_type'] == "backtest":
        StrategyTest(main_engine).run()
    else:
        pass
        # Strategy(main_engine).run()

if __name__ == '__main__':
    main()
