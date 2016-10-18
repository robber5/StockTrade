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


class StrategyEngine(object):
    """策略引擎"""
    def __init__(self):
        settingFileName = 'CTA_setting.json'
        settingFileName = os.getcwd() + '/ctaAlgo/' + settingFileName
