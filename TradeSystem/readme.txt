策略开发与回测系统

说明：
1、配置文件Trade_Setting.json
2、主程序为tsMain.py——>读取配置文档(Trade_Setting.json)——>载入数据引擎(mainEngine)——>完成数据（因子和基础数据）更新后——>策略模块(tsTrade.py)
3、其中择时系统(selectStrategy文件夹中的tsStrategy)通过继承tsTrade.py后重载timing_strategy方法实现
3、策略目前分为择时(selectStrategy)\Alpha(alphaModel、positionManage组成)两大部分，具体交易操作在交易控制模块完成(operateManage)
4、tsAccount记录了账户的实时情况，贯穿于整个程序
5、数据的输出除了持仓的输出在tsAccount.py当中外，其他的数据输出全部在tsTrade.py的最后来控制
6、输出项有hedge\alpha\Timing的持仓及调仓