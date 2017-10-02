"""
end-of-event report plotting tools
"""

##### IMPORTANT matplotlib declarations must always be FIRST to make sure that
##### matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ion()

from datetime import timedelta
import numpy as np
import pandas as pd
#import seaborn

import ColumnPlotter as plotter
import genproc as proc
import querySenslopeDb as qdb
import rtwindow as rtw

mpl.rcParams['xtick.labelsize'] = 8
mpl.rcParams['ytick.labelsize'] = 8

def zeroed(df, column):
    df['zeroed_'+column] = df[column] - df[column].values[0]
    return df

# surficial data
def get_surficial_df(site, start, end):

    query = "SELECT timestamp, site_id, crack_id, meas FROM gndmeas"
    query += " WHERE site_id = '%s'" % site
    query += " AND timestamp <= '%s'"% end
    query += " AND timestamp > '%s'" % start
    query += " ORDER BY timestamp"
    
    df = qdb.GetDBDataFrame(query)    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['crack_id'] = map(lambda x: x.upper(), df['crack_id'])
    
    marker_df = df.groupby('crack_id', as_index=False)
    df = marker_df.apply(zeroed, column='meas')
    
    return df

# surficial plot
def plot_surficial(ax, df, marker_lst):
    if marker_lst == 'all':
        marker_lst = set(df.crack_id)
    for marker in marker_lst:
        marker_df = df[df.crack_id == marker]
        ax.plot(marker_df.timestamp, marker_df.zeroed_meas, marker='o',
                label=marker, alpha=1)
    ax.set_ylabel('Displacement\n(cm)', fontsize='small')
    ax.set_title('Surficial Ground Displacement', fontsize='medium')
    ncol = (len(set(df.crack_id)) + 3) / 4
    ax.legend(loc='upper left', ncol=ncol, fontsize='x-small', fancybox = True, framealpha = 0.5)
    ax.grid()

# rainfall data
def get_rain_df(rain_gauge, start, end):
    rain_df = qdb.GetRawRainData(rain_gauge, fromTime=start, toTime=end)
    
    rain_df = rain_df[rain_df.rain >= 0]
    rain_df = rain_df.set_index('ts')
    rain_df = rain_df.resample('30min').sum()
    
    rain_df['one'] = rain_df.rain.rolling(window=48, min_periods=1, center=False).sum()
    rain_df['one'] = np.round(rain_df.one, 2)
    rain_df['three'] = rain_df.rain.rolling(window=144, min_periods=1, center=False).sum()
    rain_df['three'] = np.round(rain_df.three, 2)
    
    rain_df = rain_df.reset_index()
    
    return rain_df

# rainfall plot
def plot_rain(ax, df, site):
    ax.plot(df.ts, df.one, color='green', label='1-day cml', alpha=1)
    ax.plot(df.ts,df.three, color='blue', label='3-day cml', alpha=1)
#    ax2=ax.twinx()
#    width=0.01
#    ins_df = df.dropna()
#    ax2.bar(ins_df.ts, ins_df.rain, width, color='k', label = '30min rainfall')
    
    query = "SELECT * FROM rain_props where name = '%s'" %site
    twoyrmax = qdb.GetDBDataFrame(query)['max_rain_2year'].values[0]
    halfmax = twoyrmax/2
    
    ax.plot(df.ts, [halfmax]*len(df.ts), color='green', label='half of 2-yr max', alpha=1, linestyle='--')
    ax.plot(df.ts, [twoyrmax]*len(df.ts), color='blue', label='2-yr max', alpha=1, linestyle='--')
    
    ax.set_title("%s Rainfall Data" %site.upper(), fontsize='medium')  
    ax.set_ylabel('1D, 3D Rain\n(mm)', fontsize='small')  
    ax.legend(loc='upper left', fontsize='x-small', fancybox = True, framealpha = 0.5)
    ax.grid()

# subsurface data
def get_tsm_data(tsm_name, start, end, plot_type, node_lst):
    
    col = qdb.GetSensorList(tsm_name)[0]
    
    window, config = rtw.getwindow(pd.to_datetime(end))

    window.start = pd.to_datetime(start)
    window.offsetstart = window.start - timedelta(days=(config.io.num_roll_window_ops*window.numpts-1)/48.)
    
    if plot_type == 'cml':
        config.io.to_smooth = 1
        config.io.to_fill = 1
    else:
        config.io.to_smooth = 1
        config.io.to_fill = 0

    monitoring = proc.genproc(col, window, config, 'bottom', comp_vel=False)
    df = monitoring.disp_vel.reset_index()[['ts', 'id', 'xz', 'xy']]
    df = df.loc[(df.ts >= window.start)&(df.ts <= window.end)]
    df = df.sort_values('ts')
    
    if plot_type == 'cml':
        xzd_plotoffset = 0
        if node_lst != 'all':
            df = df[df.id.isin(node_lst)]
        df = plotter.cum_surf(df, xzd_plotoffset, col.nos)
    else:
        node_df = df.groupby('id', as_index=False)
        df = node_df.apply(zeroed, column='xz')
        df['zeroed_xz'] = df['zeroed_xz'] * 100
        node_df = df.groupby('id', as_index=False)
        df = node_df.apply(zeroed, column='xy')
        df['zeroed_xy'] = df['zeroed_xy'] * 100
    
    return df

# subsurface cumulative displacement plot
def plot_cml(ax, df, axis, tsm_name):
    ax.plot(df.index, df[axis].values)
    ax.set_ylabel('Cumulative\nDisplacement\n(m)', fontsize='small')
    ax.set_title('%s Subsurface Cumulative %s Displacement' %(tsm_name.upper(), axis.upper()), fontsize='medium')
    ax.grid()
    
# subsurface displacemnt plot
def plot_disp(ax, df, axis, node_lst, tsm_name):
    for node in node_lst:
        node_df = df[df.id == node]
        ax.plot(node_df.ts, node_df['zeroed_'+axis].values, label='Node '+str(node))
    ax.set_ylabel('Displacement\n(cm)', fontsize='small')
    ax.set_title('%s Subsurface %s Displacement' %(tsm_name.upper(), axis.upper()), fontsize='medium')
    ncol = (len(node_lst) + 3) / 4
    ax.legend(loc='upper left', ncol=ncol, fontsize='x-small', fancybox = True, framealpha = 0.5)
    ax.grid()

def plot_single_event(ax, ts):
    ax.axvline(ts, color='red', linestyle='--', alpha=1)    

def main(site, start, end, rainfall_props, surficial_props, subsurface_props, event_lst):
    # count of subplots in subsurface displacement
    disp = subsurface_props['disp']['to_plot']
    num_disp = 0
    disp_plot = subsurface_props['disp']['disp_tsm_axis']
    disp_plot_key = disp_plot.keys()
    for i in disp_plot_key:
        num_disp += len(disp_plot[i].keys())
    disp = [disp] * num_disp

    # count of subplots in subsurface displacement
    cml = subsurface_props['cml']['to_plot']
    num_cml = 0
    cml_plot = subsurface_props['cml']['cml_tsm_axis']
    cml_plot_key = cml_plot.keys()
    for i in cml_plot_key:
        num_cml += len(cml_plot[i].keys())
    cml = [cml] * num_cml

    # total number of subplots in subsurface
    subsurface = disp + cml

    # total number of subplots
    num_subplots = ([rainfall_props['to_plot'], surficial_props['to_plot']] + 
                subsurface).count(True)
    subplot = num_subplots*101+10

    x_size = 8
    y_size = 5*num_subplots
    fig=plt.figure(figsize = (x_size, y_size))

    if rainfall_props['to_plot']:
        rain = get_rain_df(rainfall_props['rain_gauge'], start, end)
        ax = fig.add_subplot(subplot)
        plot_rain(ax, rain, site)
        for event in event_lst:
            plot_single_event(ax, event)
        
    if surficial_props['to_plot']:
        surficial = get_surficial_df(site, start, end)
        try:
            ax = fig.add_subplot(subplot-1, sharex=ax)
            subplot -= 1
        except:
            ax = fig.add_subplot(subplot)
        if rainfall_props['to_plot']:
            ax.xaxis.set_visible(False)
        plot_surficial(ax, surficial, surficial_props['markers'])
        for event in event_lst:
            plot_single_event(ax, event)

    if subsurface_props['disp']['to_plot']:
        disp = subsurface_props['disp']['disp_tsm_axis']
        for tsm_name in disp.keys():
            tsm_data = get_tsm_data(tsm_name, start, end, 'disp', 'all')
            return_data = tsm_data
            axis_lst = disp[tsm_name]
            for axis in axis_lst.keys():
                try:
                    ax = fig.add_subplot(subplot-1, sharex=ax)
                    subplot -= 1
                except:
                    ax = fig.add_subplot(subplot)
                ax.xaxis.set_visible(False)
                plot_disp(ax, tsm_data, axis, axis_lst[axis], tsm_name)
                for event in event_lst:
                    plot_single_event(ax, event)

    if subsurface_props['cml']['to_plot']:
        cml = subsurface_props['cml']['cml_tsm_axis']
        for tsm_name in cml.keys():
            node_lst = []
            for node in cml[tsm_name].values():
                if node != 'all':
                    node_lst += node
                else:
                    node_lst += [node]
            if 'all' in node_lst:
                node_lst = 'all'
            else:
                node_lst = list(set(node_lst))
            tsm_data = get_tsm_data(tsm_name, start, end, 'cml', node_lst)
            axis_lst = cml[tsm_name]
            for axis in axis_lst.keys():
                try:
                    ax = fig.add_subplot(subplot-1, sharex=ax)
                    subplot -= 1
                except:
                    ax = fig.add_subplot(subplot)
                ax.xaxis.set_visible(False)
                plot_cml(ax, tsm_data, axis, tsm_name)
                for event in event_lst:
                    plot_single_event(ax, event)

    ax.set_xlim([start, end])
    fig.subplots_adjust(top=0.9, right=0.95, left=0.15, bottom=0.05, hspace=0.3)
    fig.suptitle(site.upper() + " Event Timeline",fontsize='x-large')
    plt.savefig(site + "_event_timeline", dpi=200,mode='w')#, 
#        facecolor='w', edgecolor='w',orientation='landscape')

    return return_data

############################################################

if __name__ == '__main__':
    
    site = 'pin'
    start = '2017-09-11 12:00'
    end = '2017-09-26 12:00'
    
    # annotate events
    event_lst = []#'2017-09-11 12:30:00', '2017-09-12 06:30:00']
    
    # rainfall plot
    rainfall = True                                 ### True if to plot rainfall
    rain_gauge = 'umiw'                           ### specifiy rain gauge
    rainfall_props = {'to_plot': rainfall, 'rain_gauge': rain_gauge}

    # surficial plot
    surficial = False                  ### True if to plot surficial
    markers = ['A', 'B']    ### specifiy markers; 'all' if all markers
    surficial_props = {'to_plot': surficial, 'markers': markers}
    
    # subsurface plot
    
    # subsurface displacement
    disp = True                    ### True if to plot subsurface displacement
    ### specifiy tsm name and axis; 'all' if all nodes
    disp_tsm_axis = {'umita': {'xz': range(1,21)}}
    
    # subsurface cumulative displacement
    cml = False          ### True if to plot subsurface cumulative displacement
    ### specifiy tsm name and axis; 'all' if all nodes
    cml_tsm_axis = {'magta': {'xz': 'all'},
                            'magtb': {'xy': range(11,16), 'xz': range(11,16)}}
    
    subsurface_props = {'disp': {'to_plot': disp, 'disp_tsm_axis': disp_tsm_axis},
                        'cml': {'to_plot': cml, 'cml_tsm_axis': cml_tsm_axis}}
    
    df = main(site, start, end, rainfall_props, surficial_props, subsurface_props, event_lst)
    
################################################################################
    
#    def ts_range(df, lst):
#        lst.append((pd.to_datetime(df['ts'].values[0]), pd.to_datetime(df['ts'].values[0]) + timedelta(hours=0.5)))
#        return lst
#    
#    def stitch_intervals(ranges):
#        result = []
#        cur_start = -1
#        cur_stop = -1
#        for start, stop in sorted(ranges):
#            if start != cur_stop:
#                result.append((start,stop))
#                cur_start, cur_stop = start, stop
#            else:
#                result[-1] = (cur_start,stop)
#                cur_stop = max(cur_stop,stop)
#        return result
#    
#    rain = get_rain_df('pintaw', '2016-03-15', '2017-09-13')
#    query = "SELECT * FROM rain_props where name = '%s'" %'pin'
#    twoyrmax = qdb.GetDBDataFrame(query)['max_rain_2year'].values[0]
#    halfmax = twoyrmax/2
#    
#    halfT = rain[(rain.one >= halfmax/2)|(rain.three >= halfmax)]
#    
#    lst = []
#    halfTts = halfT.groupby('ts', as_index=False)
#    rain_ts_range = halfTts.apply(ts_range, lst=lst)
#    rain_ts_range = rain_ts_range[0]
#    rain_ts_range = rain_ts_range[1::]
#    halfT_range = stitch_intervals(rain_ts_range)