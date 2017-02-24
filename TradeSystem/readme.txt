策略开发与回测系统

说明：
1、配置文件Trade_Setting.json
2、主程序为tsMain.py,在该文件中读取配置文档后载入数据引擎，完成数据（因子和基础数据）更新后进入策略模块
3、策略目前分为择时（selectStrategy）、Alpha（alphaModel、positionManage组成）两大部分，具体交易操作在交易控制模块完成(tradeControl)
4、tsAccout记录了账户的实时情况，贯穿于整个程序始终
5、数据的输出除了持仓的输出在tsAccount.py当中外，其他的数据输出全部在tsTrade.py的最后来控制

