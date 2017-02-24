# coding=utf-8

import json
from TradeSystem.selectStrategy.tsStrategy import StrategyTest
from TradeSystem.tsEngine import MainEngine
from TradeDataRecorder.drDataUpdate import DataUpdate
from TradeDataRecorder.drBasisDateUpdate import BasisDataUpdate


def main():
    """主程序入口"""
    # 读取配置文件
    setting = dict
    try:
        f = file("Trade_Setting.json")
        setting = json.load(f)
        print(setting['StrategyTitle'].encode('utf-8') + " start")
    except Exception, ex:
        print('profile loading error：' + ex.message)

    # 载入配置到交易模块
    main_engine = MainEngine(
        setting['MS_host'].encode('utf-8'),
        setting['MS_user'].encode('utf-8'),
        setting['MS_pwd'].encode('utf-8'),
        setting['MS_db'].encode('utf-8'),
        setting['sqlite_DB'].encode('utf-8')
    )

    """
    # update 基础因子表
    # DataUpdate(main_engine).update_industry_change_table('industry_change_1w', '1w')
    # DataUpdate(main_engine).update_day_factors_table('day_factors_update')
    # DataUpdate(main_engine).update_week_factors_table('week_factors_update')
    # DataUpdate(main_engine).update_month_factors_table('month_factors_update')
    # DataUpdate(main_engine).update_quarter_factors_table('quarter_factors_update')
    # update 各个因子按时间轴对齐的表: rolling_table
    # DataUpdate(main_engine).update_all_factors_rolling_table('all_F_1W_rolling_update', '1w')
    # DataUpdate(main_engine).update_all_factors_rolling_table_FAST_FOR_SHORTERM('all_F_1M_rolling_update', '1m')
    # 根据 rolling_table 去 update weighting table
    # DataUpdate(main_engine).update_weighting_table('all_F_1M_rolling_update', 'all_F_1M_weighting_update')
    # 标准化表
    # DataUpdate(main_engine).update_standard_table('all_F_1M_rolling_update', 'all_F_1M_rolling_update_std')
    # DataUpdate(main_engine).update_standard_table('all_F_1M_weighting_update', 'all_F_1M_weighting_update_std')
    # BasisDataUpdate(main_engine).basis_date_update()
    """

    if setting['engine_type'] == "backtest":
        # 运行
        # 初始资金
        StrategyTest(main_engine).run()

    # else:
    #     Strategy(main_engine).run()

if __name__ == '__main__':
    main()
