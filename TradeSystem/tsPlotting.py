# coding=utf-8

import matplotlib
matplotlib.use('TKAgg')
import matplotlib.pyplot as plt


def plot_strategy(price_data, indicators={}, deals=[], curve=[], marks=[]):
    """
        显示回测结果。
    """
    print "plotting.."
    fig = plt.figure()
    frame = widgets.TechnicalWidget(fig, price_data)
    frame.init_layout(
        50,         # 窗口显示k线数量。
        4, 1     # 两个1:1大小的窗口
    )

    # 添加k线
    kwindow = widgets.CandleWindow("kwindow", price_data, 100, 50)
    frame.add_widget(0, kwindow, True)
    # 交易信号。
    if deals:
        signal = mplots.TradingSignalPos(price_data, deals, lw=2)
        frame.add_technical(0, signal)
    if len(curve) > 0:
        curve = Line(curve)
        frame.add_technical(0, curve, True)
    frame.add_technical(1, Volume(price_data.open, price_data.close, price_data.volume))
    # 添加指标
    for name, in_dic in indicators.iteritems():
        frame.add_technical(0, in_dic)
    # 绘制标志
    if marks:
        if marks[0]:
            # plot lines
            for name, values in marks[0].iteritems():
                v = values[0]
                ith_ax = v[0]
                t_win_x = v[1]
                line_pieces = [[v[2]], [v[3]], v[4], v[5], v[6]]
                line = []
                for v in values[1:]:
                    # @TODO 如果是带“点”的，以点的特征聚类，会减少indicator对象的数目
                    x, y, style, lw, ms = v[2], v[3], v[4], v[5], v[6]
                    if style != line_pieces[2] or lw != line_pieces[3] or ms != line_pieces[4]:
                        line.append(line_pieces)
                        line_pieces = [[x], [y], style, lw, ms]
                    else:
                        line_pieces[0].append(x)
                        line_pieces[1].append(y)
                line.append(line_pieces)
                for v in line:
                    # @TODO 这里的style明确指出有点奇怪，不一致。
                    x, y, style, lw, mark_size = v[0], v[1], v[2], v[3], v[4]
                    curve = LineWithX(x, y, style=style, lw=lw, ms=mark_size)
                    frame.add_technical(ith_ax, curve, t_win_x)
        if marks[1]:
            # plot texts
            for name, values in marks[1].iteritems():
                for v in values:
                    ith_ax, x, y, text = v[0], v[1], v[2], v[3]
                    color, size, rotation = v[4], v[5], v[6]
                    frame.plot_text(name, ith_ax, x, y, text, color, size, rotation)
    frame.draw_widgets()
    plt.show()



