from django.shortcuts import render
# from django.http import HttpResponse
# import plotly as py
# import plotly.graph_objs as go
# import datetime
from visual_plotly.data_statistic_plot import ChartPlot
chart = ChartPlot()


# pyplt = py.offline.plot

def charts(request):
    context = {}
    context['platform_graph_plot']= chart.platform_graph()
    context['search_trend']= chart.search_trend()
    context['zy_graph_trend'] = chart.zy_graph_trend()
    context['zy_graph'] = chart.zy_graph()
    context['zy_graph_m0'] = chart.zy_graph_m0()
    context['zy_graph_m0_2'] = chart.zy_graph_m0_2()
    context['zy_graph_m1'] = chart.zy_graph_m1()
    context['zy_graph_m1_2'] = chart.zy_graph_m1_2()
    context['zy_graph_m2'] = chart.zy_graph_m2()
    context['zy_graph_m2_2'] = chart.zy_graph_m2_2()
    context['zy_graph_m3'] = chart.zy_graph_m3()
    context['zy_graph_m3_2'] = chart.zy_graph_m3_2()

    return render(request, 'index.html', context=context)

def charts2(request):
    context = {}
    context['zy_graph_m4'] = chart.zy_graph_m4()
    context['zy_graph_m4_2'] = chart.zy_graph_m4_2()
    context['zy_graph_m5'] = chart.zy_graph_m5()
    context['zy_graph_m5_2'] = chart.zy_graph_m5_2()
    context['zy_graph_m6'] = chart.zy_graph_m6()
    context['zy_graph_m6_2'] = chart.zy_graph_m6_2()
    context['zy_graph_m7'] = chart.zy_graph_m7()
    context['zy_graph_m7_2'] = chart.zy_graph_m7_2()
    context['zy_graph_m8'] = chart.zy_graph_m8()
    context['zy_graph_m8_2'] = chart.zy_graph_m8_2()
    context['zy_graph_m9'] = chart.zy_graph_m9()
    context['zy_graph_m9_2'] = chart.zy_graph_m9_2()

    return render(request, 'index2.html', context=context)
