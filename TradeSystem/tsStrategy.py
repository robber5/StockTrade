# encoding: UTF-8

"""
可以使用回测或者实盘
"""

import json
import os
from datetime import datetime, timedelta
from tsHedgeEngine import *
import pandas as py
from tsMssql import *
from tsTrade import Trade


class StrategyTest(Trade):
    """策略"""
    def __init__(self, _main_engine):

        super(StrategyTest, self).__init__()
        settingFileName = 'CTA_setting.json'
        settingFileName = os.getcwd() + '/ctaAlgo/' + settingFileName

    def run(self):
        pass

    def handle_date(self, _account):
        pass
