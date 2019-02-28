# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 17:40:51 2019

@author: chenzx
"""

import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import warnings

warnings.filterwarnings("ignore")
# 读取采购明细

start_time = '2019-02-18'
end_time = '2019-02-28'

engine_1 = create_engine(
    "mysql+mysqldb://{}:{}@{}/{}?charset=utf8".format('yhxc', 'yhxc1234', '10.8.30.104', 'purc_details'))

sql_cmd_1 = '''
                SELECT * FROM `2019年平台采购明细` 
                where `创建时间` between "%s" and "%s" 
                ''' % (start_time, end_time)

purc_data = pd.read_sql(sql=sql_cmd_1, con=engine_1)

time_list = purc_data['创建时间'].sort_values(ascending=True).drop_duplicates()

# start_time = purc_data['创建时间'].astype(str).min()
# end_time = purc_data['创建时间'].astype(str).max()

windows_h = 1080
windows_w = 1960

adv_mafs_list = ['TI', 'ADI', 'ON', 'ST', 'INFINEON', 'VISHAY', 'SAMSUNG', 'MURATA',
                 'TE', 'NXP']

def qty_type(y, x):
    if y != '被动件':
        if x <= 20:
            return "实验样品"
        elif x <= 100:
            return "设计样品"
        elif x <= 500:
            return "工程样品"
        elif x <= 1000:
            return "小批量"
        elif x <= 3000:
            return "中批量"
        elif x <= 12000:
            return "大批量1"
        elif x <= 24000:
            return "大批量2"
        elif x <= 48000:
            return "大批量3"
        else:
            return "大批量4"
    else:
        if x <= 100:
            return "实验样品"
        elif x <= 500:
            return "设计样品"
        elif x <= 2500:
            return "工程样品"
        elif x <= 5000:
            return "小批量"
        elif x <= 15000:
            return "中批量"
        elif x <= 60000:
            return "大批量1"
        elif x <= 120000:
            return "大批量2"
        elif x <= 240000:
            return "大批量3"
        else:
            return "大批量4"


purc_data['需求区间'] = purc_data.apply(lambda x: qty_type(x['更正产品线'], x['数量']), axis=1)

# 读取某一月份搜索明细
engine_2 = create_engine(
    "mysql+mysqldb://{}:{}@{}/{}?charset=utf8".format('yhxc', 'yhxc1234', '10.8.30.104', 'daily_search'))

sql_cmd_2 = '''
            SELECT
            	sum( `搜索次数` ) AS `search_count`,
            	`日期` AS `date` 
            FROM
            	`search_data(join)` 
            where `排名分类` = 'TOP2W'
            AND `日期` BETWEEN "%s" and "%s"
            GROUP BY
            	`date` 
            ORDER BY
            	`date`
            ''' % (start_time, end_time)

sql_cmd_3 = '''
            SELECT
            	sum( `搜索次数` ) AS `search_count`,
            	`日期` AS `date` 
            FROM
            	`search_data(join)` 
            where `日期` BETWEEN "%s" and "%s"
            GROUP BY
            	`date`
            order by `date`
            ''' % (start_time, end_time)

# sql中的字符格式化输出为%Y-%m，但Python会将单一的%转义为格式化字符串，需要用%%来代替%
daily_search = pd.read_sql(sql=sql_cmd_3, con=engine_2)
daily_search['avg_per_day'] = sum(daily_search['search_count']) / len(daily_search['date'])
daily_search_top2w = pd.read_sql(sql=sql_cmd_2, con=engine_2)
daily_search_top2w['avg_per_day'] = sum(daily_search_top2w['search_count']) / len(daily_search_top2w['date'])

# 读取TOP2W型号
sql_cmd_4 = '''
            select `产品型号` from `top2w`
            '''

top2w_pno = pd.read_sql(sql=sql_cmd_4, con=engine_2)

# 匹配销售型号是否为TOP2W型号
purc_data['TOP2W'] = purc_data['产品型号'].isin(top2w_pno['产品型号'])

qty_type_list = ["实验样品", "设计样品", "工程样品", "小批量", "中批量",
                 "大批量1", "大批量2", "大批量3", "大批量4"]

# data1['qty_type'] = data1['qty_type'].astype('category').cat.set_categories(qty_type_list)
#
# data1 = data1.sort_values(by=['qty_type'], ascending=True)

# 自营采购明细
zy_purc_data = purc_data[purc_data['业务类型'] != '代购']

# 厂牌搜索数据库
engine_3 = create_engine("mysql+mysqldb://{}:{}@{}/{}?charset=utf8".format(
    'yhxc', 'yhxc1234', '10.8.30.104', 'daily_search'))

import plotly as py

pyplt = py.offline.plot
import plotly.graph_objs as go


# import os
# CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
# py.init_notebook_mode(connected=True)

class ChartPlot:
    def __str__(self):
        pass

    def platform_graph(self):
        purc_top2w = purc_data[purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})
        purc_all = purc_data.groupby('需求区间', as_index=False).agg({'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table2 = pd.DataFrame({'需求区间': qty_type_list}).merge(purc_all, on='需求区间', how='left')

        table2['percentage'] = table2['销售额(USD)'] / table2['销售额(USD)'].sum()

        table1 = pd.DataFrame({'需求区间': qty_type_list}).merge(purc_top2w, on='需求区间', how='left')

        table1['percentage'] = table1['销售额(USD)'] / table2['销售额(USD)'].sum()

        table1['percentage'] = table1['percentage'].fillna(0)
        table2['percentage'] = table2['percentage'].fillna(0)

        table2 = table2.rename(columns={'产品型号': '型号次'})
        table1 = table1.rename(columns={'产品型号': '型号次'})

        trace0 = go.Bar(
            x=table2['需求区间'],
            y=table2['型号次'],
            name='全平台',
            width=table2['percentage'] * 3
        )

        trace1 = go.Bar(
            x=table1['需求区间'],
            y=table1['型号次'] * (-1),
            name='TOP2W全平台',
            width=table1['percentage'] * 3
        )

        data = [trace0, trace1]

        layout = {
            'xaxis': {'tickfont': {'size': 30}},
            'yaxis': {'tickfont': {'size': 30}},
            'barmode': 'relative',
            'title': '<b>平台销售各需求区间分布图(%s至%s)</b>' % (start_time, end_time),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'},
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(height=windows_h, width=windows_w
                             )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='全平台销售')

    def search_trend(self):
        trace2 = go.Scatter(
            x=daily_search['date'],
            y=daily_search['search_count'],
            marker=dict(
                color='rgb(16, 118, 203)',
                size=10
            ),
            name='总搜索次数',
            line=dict(
                width=4
            )
        )

        trace3 = go.Scatter(
            x=daily_search_top2w['date'],
            y=daily_search_top2w['search_count'],
            marker=dict(
                color='rgba(255,204,102,1)',
                size=10
            ),
            name='TOP2W型号搜索次数',
            line=dict(
                width=4
            )

        )

        data = [trace2, trace3]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category'
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>平台型号搜索趋势<br>(全平台平均每日%s, TOP2W平均每日%s)</b>' % (
            int(daily_search['avg_per_day'][0]), int(daily_search_top2w['avg_per_day'][0])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='全平台搜索')

    def zy_graph_trend(self):
        table1 = zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)['销售额(USD)'].sum().sort_values(
            ['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')

        table1_2w = zy_purc_data[zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'
            ),
            name='自营销售额(USD)',
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'
            ),
            name='自营TOP2W销售额(USD)',
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category'
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>销售额趋势(%s至%s)<br>(今日自营$%s, TOP2W $%s)</b>' % (
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph(self):
        zy_purc_top2w = zy_purc_data[zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = zy_purc_data.groupby('需求区间', as_index=False).agg({'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / daily_search_top2w['search_count'].sum()

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>TOP2W型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m0(self):
        mafs = adv_mafs_list[0]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m0_2(self):
        mafs = adv_mafs_list[0]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m1(self):
        mafs = adv_mafs_list[1]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m1_2(self):
        mafs = adv_mafs_list[1]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m2(self):
        mafs = adv_mafs_list[2]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m2_2(self):
        mafs = adv_mafs_list[2]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m3(self):
        mafs = adv_mafs_list[3]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m3_2(self):
        mafs = adv_mafs_list[3]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m4(self):
        mafs = adv_mafs_list[4]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m4_2(self):
        mafs = adv_mafs_list[4]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m5(self):
        mafs = adv_mafs_list[5]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m5_2(self):
        mafs = adv_mafs_list[5]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m6(self):
        mafs = adv_mafs_list[6]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m6_2(self):
        mafs = adv_mafs_list[6]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m7(self):
        mafs = adv_mafs_list[7]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m7_2(self):
        mafs = adv_mafs_list[7]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m8(self):
        mafs = adv_mafs_list[8]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m8_2(self):
        mafs = adv_mafs_list[8]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m9(self):
        mafs = adv_mafs_list[9]

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        table1 = mafs_zy_purc_data[['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1 = pd.DataFrame({'创建时间': time_list}).merge(table1, on='创建时间', how='left')
        table1['销售额(USD)'] = table1['销售额(USD)'].fillna(0)

        table1_2w = \
        mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']][['销售额(USD)', '创建时间']].groupby(['创建时间'], as_index=False)[
            '销售额(USD)'].sum().sort_values(['创建时间'])

        table1_2w = pd.DataFrame({'创建时间': time_list}).merge(table1_2w, on='创建时间', how='left')
        table1_2w['销售额(USD)'] = table1_2w['销售额(USD)'].fillna(0)

        trace1 = go.Bar(
            x=table1['创建时间'],
            y=table1['销售额(USD)'],
            marker=dict(
                color='rgba(51,102,153,1)'),
            name='%s自营销售额(USD)' % mafs,
            opacity=0.6,
            #    title = '销售额趋势(今日销售额$%s)' % int(table1['销售额(USD)'].tolist()[-1])
        )

        trace1_2w = go.Bar(
            x=table1_2w['创建时间'],
            y=table1_2w['销售额(USD)'],
            marker=dict(
                color='rgba(255,204,102,1)'),
            name='%s自营TOP2W销售额(USD)' % mafs,
        )

        data = [trace1, trace1_2w]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30},
                'type': 'category',
                'automargin': True

            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'group',
            'title': '<b>%s销售额趋势(%s至%s)<br>(今日自营$%s,TOP2W $%s)</b>' % (
                mafs,
                start_time, end_time,
                int(table1['销售额(USD)'].tolist()[-1]), int(table1_2w['销售额(USD)'].tolist()[-1])),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

    def zy_graph_m9_2(self):
        mafs = adv_mafs_list[9]

        sql_mafs_1 = '''
                        SELECT
                        	sum( `搜索次数` ) AS `search_count`
                        FROM
                        	`search_data(comb)` 
                        where `排名分类` = 'TOP2W' and `厂牌`= "%s"
                        AND `日期` BETWEEN "%s" and "%s"
                    ''' % (mafs, start_time, end_time)

        mafs_search = pd.read_sql(sql=sql_mafs_1, con=engine_3)

        mafs_zy_purc_data = zy_purc_data[zy_purc_data['云汉标准厂牌'] == mafs]

        zy_purc_top2w = mafs_zy_purc_data[mafs_zy_purc_data['TOP2W']].groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        zy_purc_all = mafs_zy_purc_data.groupby('需求区间', as_index=False).agg(
            {'产品型号': np.count_nonzero, '销售额(USD)': np.sum})

        table3 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_top2w, on='需求区间', how='left')

        table4 = pd.DataFrame({'需求区间': qty_type_list}).merge(zy_purc_all, on='需求区间', how='left')

        table3['percentage'] = table3['销售额(USD)'] / table4['销售额(USD)'].sum()

        table4['percentage'] = table4['销售额(USD)'] / table4['销售额(USD)'].sum()

        table3['percentage'] = table3['percentage'].fillna(0)
        table4['percentage'] = table4['percentage'].fillna(0)

        table3 = table3.rename(columns={'产品型号': '型号次'})
        table4 = table4.rename(columns={'产品型号': '型号次'})

        top2w_order_rate = zy_purc_top2w['产品型号'].sum() / mafs_search['search_count'][0]

        trace3 = go.Bar(
            x=table4['需求区间'],
            y=table4['型号次'],
            marker=dict(
                color='rgba(58,154,217,1)'),
            name='自营',
            width=table4['percentage'] * 3
        )

        trace4 = go.Bar(
            x=table3['需求区间'],
            y=table3['型号次'] * (-1),
            marker=dict(
                color='rgba(41,171,164,1)'),
            name='TOP2W自营',
            width=table3['percentage'] * 3
        )

        data = [trace3, trace4]

        layout = {
            'xaxis': {
                'tickfont': {'size': 30}
            },
            'yaxis': {
                'tickfont': {'size': 30}
            },
            'barmode': 'relative',
            'title': '<b>%s型号自营销售各需求区间分布图(%s至%s)<br>(合计销售额$%s,搜索命中成单率%.2f%%)</b>' % (
                mafs,
                start_time, end_time,
                int(table3['销售额(USD)'].sum()), top2w_order_rate * 100),
            'font': {
                'family': '\"Open Sans\", verdana, arial, sans-serif',
                'size': 30,
                'color': '#444'
            },
            'legend': {
                'orientation': 'h',
                'x': 0.5,
                'y': 0.95
            }
        }

        fig = go.Figure(data=data, layout=layout)
        fig['layout'].update(
            height=windows_h, width=windows_w
        )
        div = pyplt(fig, output_type='div', auto_open=False, show_link=False, include_plotlyjs=False)
        return div

    #        pyplt(fig, filename='自营销售')

if __name__ == 'main':
    charts = ChartPlot()
