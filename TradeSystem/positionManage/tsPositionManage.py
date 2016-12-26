# coding=utf-8

import numpy as np


class PositionEngine:
    def __init__(self):
        # 单只股票最大可亏损比例
        self.risk_ratio_once = 0.0005
        self.dic_risk_ratio = {}
        self.target_pool = []
        self.active = False

    def position_manage(self, current_day):
        if self.active:
            pass
            # todo 这里增加

    def stock_risk_ration_init(self):
        for s in self.target_pool:
            self.dic_risk_ratio[s] = np.ones(1) * self.risk_ratio_once

    def switch_Engine_Status(self):
        """控制仓控引擎开关"""
        self.active = not self.active
