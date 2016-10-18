# encoding: UTF-8


class HedgeEngine(object):
    """对冲引擎"""
    def __init__(self):
        settingFileName = 'CTA_setting.json'
        settingFileName = os.getcwd() + '/ctaAlgo/' + settingFileName
