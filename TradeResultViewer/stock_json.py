# coding=utf-8

from flask import make_response
import json
import time
from flask import Flask, render_template
import pandas as pd
import datetime
from TradeSystem.tradeSystemBase.tsMssql import MSSQL

app = Flask(__name__)


def get_access_control(_json):
    """允许跨域访问"""

    rst = make_response(_json)

    rst.headers['Access-Control-Allow-Origin'] = "*"
    rst.headers['content-type'] = "text/javascript; charset=utf-8"

    return rst


@app.route('/<name>')
def index(name):
    return render_template('index.html', name=name)


@app.route('/stock/<code>')
def stock(code):
    sql_tuple = MSSQL(host="127.0.0.1", user="sa", pwd="windows-999", db="stocks").execquery(
        "select a.date, adjust_price_f from (select * from v_trade_date) a left outer join(select * from stock_data where code = '"+code+"')  b on a.[date] = b.[date] where a.date >= (select top 1 date from stock_data where code = '"+code+"') order by a.date")

    list_stock = []

    stop_price = 0

    for s in sql_tuple:
        if s[1] is not None:
            stop_price = s[1]
        list_date = [
            time.mktime(s[0].timetuple()) * 1000, stop_price]
        list_stock.append(list_date)

    _json = json.dumps(list_stock)

    return get_access_control(_json)


@app.route('/zig/<code>')
def zig(code):
    df = pd.read_csv('zig_log.csv', index_col='date')
    df = df[df['code'] == code]
    dic = df.to_dict(orient='index')

    dic = sorted(dic.iteritems(), key=lambda d: d[0])

    list_stock = []

    for a in dic:
        list_date = [time.mktime(datetime.datetime.strptime(a[0], '%Y-%m-%d').timetuple()) * 1000,
                     a[1]['price']]
        list_stock.append(list_date)

    _json = json.dumps(list_stock)

    return get_access_control(_json)


@app.route('/buy/<code>')
def buy(code):
    df = pd.read_csv('operate.csv')
    df = df[df['stockcode'] == code]
    df = df[df['operatetype'] == 'buy']

    dic = df.to_dict(orient='index')

    list_stock = []

    for a in dic:
        list_date = [time.mktime(datetime.datetime.strptime(dic[a]['date'], '%Y-%m-%d').timetuple()) * 1000,
                     dic[a]['referenceprice']]
        list_stock.append(list_date)

    _json = json.dumps(list_stock)

    return get_access_control(_json)


@app.route('/sell/<code>')
def sell(code):
    df = pd.read_csv('operate.csv')
    df = df[df['stockcode'] == code]
    df = df[df['operatetype'] == 'sell']

    dic = df.to_dict(orient='index')

    list_stock = []

    for a in dic:
        list_date = [time.mktime(datetime.datetime.strptime(dic[a]['date'], '%Y-%m-%d').timetuple()) * 1000,
                     dic[a]['referenceprice']]
        list_stock.append(list_date)

    _json = json.dumps(list_stock)

    return get_access_control(_json)


@app.route('/stock_fund/<code>')
def fund(code):
    amount = 0
    buy_price = 0
    sql_tuple = MSSQL(host="127.0.0.1", user="sa", pwd="windows-999", db="stocks").execquery(
        "select a.date, adjust_price_f from (select * from v_trade_date) a left outer join(select * from stock_data where code = '" + code + "')  b on a.[date] = b.[date] where a.date >= (select top 1 date from stock_data where code = '" + code + "') order by a.date")

    fund_base = 0

    list_stock = []

    stop_price = 0

    df = pd.read_csv('operate.csv', index_col='date')

    df = df[df['stockcode'] == code]

    df_buy = df[df['operatetype'] == 'buy']

    dict_buy = df_buy.to_dict(orient='index')

    df_sell = df[df['operatetype'] == 'sell']

    buy_date = df_buy.index.values.tolist()
    sell_date = df_sell.index.values.tolist()

    for s in sql_tuple:
        s_str = datetime.datetime.strftime(s[0], '%Y-%m-%d')
        if s_str in buy_date:
            amount = dict_buy[s_str]['referencenum']
            buy_price = dict_buy[s_str]['referenceprice']
        elif s_str in sell_date:
            fund_base += amount * (stop_price - buy_price)
            amount = 0
            buy_price = 0

        if s[1] is not None:
            stop_price = s[1]
        list_date = [
            time.mktime(s[0].timetuple()) * 1000, fund_base + amount * (stop_price - buy_price)]
        list_stock.append(list_date)

    _json = json.dumps(list_stock)

    return get_access_control(_json)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
