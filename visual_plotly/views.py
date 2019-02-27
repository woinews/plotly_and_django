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
    context['zy_graph_plot']= chart.zy_graph()
    context['zy_graph_m0'] = chart.zy_graph_m0()
    context['zy_graph_m1'] = chart.zy_graph_m1()
    context['zy_graph_m2'] = chart.zy_graph_m2()
    context['zy_graph_m3'] = chart.zy_graph_m3()
    context['zy_graph_m4'] = chart.zy_graph_m4()
    context['zy_graph_m5'] = chart.zy_graph_m5()
    context['zy_graph_m6'] = chart.zy_graph_m6()
    context['zy_graph_m7'] = chart.zy_graph_m7()
    context['zy_graph_m8'] = chart.zy_graph_m8()
    context['zy_graph_m9'] = chart.zy_graph_m9()
    return render(request, 'index.html', context=context)

