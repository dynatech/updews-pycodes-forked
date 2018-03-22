##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ioff()

from datetime import timedelta
import numpy as np
import pandas as pd

import rainfallalert as ra

def stitch_intervals(ranges):
    result = []
    cur_start = -1
    cur_stop = -1
    for start, stop in sorted(ranges):
        if start != cur_stop:
            result.append((start,stop))
            cur_start, cur_stop = start, stop
        else:
            result[-1] = (cur_start,stop)
            cur_stop = max(cur_stop,stop)
    return result

def plot_shade(df, ax):
    
    """
    timestamp 
    """    
    
    ax.axvspan(pd.to_datetime(df['shaded_range'].values[0][0]),
               pd.to_datetime(df['shaded_range'].values[0][1]),
               alpha = 0.5, color='#afeeee')

def rain_subplot(rain_gauge_props, offsetstart, start, end, threshold,
                 insax, cumax, fig, site_code):
    # resampled data
    data = ra.get_resampled_data(rain_gauge_props['rainfall_gauges'].values[0],
                                     offsetstart, start, end, check_nd=False)
    if len(data) == 0:
        data = pd.DataFrame(columns=['ts', 'rain']).set_index('ts')
        
    # 1-day cumulative rainfall
    rainfall2 = data.rolling(min_periods=1, window=48).sum()
    rainfall2 = np.round(rainfall2,4)
    
    # 3-day cumulative rainfall
    rainfall3 = data.rolling(min_periods=1, window=144).sum()
    rainfall3 = np.round(rainfall3,4)

    # instantaneous, 1-day, and 3-day cumulative rainfall in one dataframe
    data['24hr cumulative rainfall'] = rainfall2.rain
    data['72hr cumulative rainfall'] = rainfall3.rain
    data = data[(data.index >= start)]
    data = data[(data.index <= end)]
    plot1 = data['rain']
    plot2 = data['24hr cumulative rainfall']
    plot3 = data['72hr cumulative rainfall']
    plot4 = threshold['half of 2yr max rainfall']
    plot5 = threshold['2yr max rainfall']
    
    
    RG_num = rain_gauge_props.index[0]
    inscurax = insax[RG_num]
    cumcurax = cumax[RG_num]
    
    # shade range without data
    try:
        nan_data = data[pd.isnull(data).rain]
        nan_data = nan_data.reset_index()
        nan_range = nan_data['ts'].apply(lambda x: (x, x+timedelta(hours=0.5)))
        shaded_range = stitch_intervals(nan_range)
        shaded_df = pd.DataFrame({'shaded_range': shaded_range})
        shaded_grp = shaded_df.groupby('shaded_range')
        shaded_grp.apply(plot_shade, ax=inscurax)
    except:
        pass

    
    try:
        # instantaneous, 1-day & 3-day cumulative rainfall
        inscurax.bar(plot1.index,plot1,width=0.01,color='r')
        cumcurax.plot(plot2.index,plot2,color='b')
        cumcurax.plot(plot3.index,plot3,color='r')
        # 1-day & 3-day cumulative rainfall threshold
        cumcurax.plot(plot4.index,plot4,color='b',linestyle='--')
        cumcurax.plot(plot5.index,plot5,color='r',linestyle='--')
        
        # trim plot if cumulative rainfall above 500mm
        b, t = cumcurax.get_ylim()
        if t > 500:
            t = 500
        cumcurax.set_ylim([b, t + 25])
        
        # rotate x-axis label (timestamp)
        for tick in inscurax.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
        for tick in cumcurax.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')

    except:
        pass
    
    ylabel = rain_gauge_props['rainfall_gauges'].values[0]
    ylabel += ' (' + str(rain_gauge_props['distance'].values[0]) + 'km)'
    ylabel = ylabel.replace('rain_noah_', 'NOAH').replace('rain_', '')
    ylabel = ylabel.replace(' (km)', '')
    inscurax.set_ylabel(ylabel, fontsize='medium')
    
    # formats major axis to Month-day
    dfmt = md.DateFormatter('%m-%d')
    inscurax.xaxis.set_major_formatter(dfmt)
    cumcurax.xaxis.set_major_formatter(dfmt)
    
def rain_stack_plot(site_code, gauges, offsetstart, start, end, tsn, threshold,
                    sc, output_path):
    
    # assigning axis name per subplot
    plt.xticks(rotation=70, size=5)       
    fig=plt.figure(figsize = (15,20))
    
    # assigning axis name for instantaneous rainfall of each rain gauge
    ins1 = fig.add_subplot(len(gauges),2,1)
    ins2 = fig.add_subplot(len(gauges),2,3, sharex=ins1, sharey=ins1)
    ins3 = fig.add_subplot(len(gauges),2,5, sharex=ins1, sharey=ins1)
    ins4 = fig.add_subplot(len(gauges),2,7, sharex=ins1, sharey=ins1)

    # assigning axis name for cumulative rainfall of each rain gauge
    cum1 = fig.add_subplot(len(gauges),2,2)
    cum2 = fig.add_subplot(len(gauges),2,4, sharex=cum1)
    cum3 = fig.add_subplot(len(gauges),2,6, sharex=cum1)
    cum4 = fig.add_subplot(len(gauges),2,8, sharex=cum1)

    insax = [ins1, ins2, ins3, ins4]
    cumax = [cum1, cum2, cum3, cum4]

    rain_gauge_props = gauges.groupby('rainfall_gauges')
    
    # plotting per rain gauge
    rain_gauge_props.apply(rain_subplot, offsetstart=offsetstart, start=start, 
                         end=end, threshold=threshold, insax=insax,
                         cumax=cumax, fig=fig, site_code=site_code) 
    
    # range of x-axis
    ins1.set_xlim([start - timedelta(hours=2), end + timedelta(hours=2)])
    cum1.set_xlim([start - timedelta(hours=2), end + timedelta(hours=2)])

    # adjusts subplots
    fig.subplots_adjust(top=0.93, right=0.8, left=0.08, bottom=0.08, hspace=0.23, wspace=0.13)
    # title of plot
    fig.suptitle(site_code.upper()+" as of "+str(end),fontsize='xx-large')

    # save plot on file path for monitoring output or event monitoring
    lgd = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='medium')
#    file_path = filepath.output_file_path(name, 'rainfall', monitoring_end=monitoring_end, 
#        trigger=trigger, end=end)
    plt.savefig(output_path+sc['fileio']['rainfall_path'] + 'rainfall_' +
                tsn + '_' + site_code, dpi=100, facecolor='w',
                edgecolor='w',orientation='landscape',mode='w',
                bbox_extra_artists=(lgd,))#, bbox_inches='tight')
#    if file_path['event'] != None:
#        plt.savefig(output_path+sc['fileio']['rainfall_path']
#            + 'rainfall_' + tsn + '_' + name, dpi=100, 
#            facecolor='w', edgecolor='w',orientation='landscape',mode='w',
#            bbox_extra_artists=(lgd,))#, bbox_inches='tight')
    plt.close()

################################     MAIN     ################################

def main(props, offsetstart, start, end, tsn, sc, output_path):

    """
    plot
    """

    #rainfall properties from siterainprops
    site_code = props['site_code'].values[0]
    twoyrmax = props['threshold_value'].values[0]
    halfmax=twoyrmax/2

    index = [start, end]
    columns=['half of 2yr max rainfall','2yr max rainfall']
    threshold = pd.DataFrame(index=index, columns=columns)
    threshold['half of 2yr max rainfall'] = halfmax  
    threshold['2yr max rainfall'] = twoyrmax

    gauges = pd.DataFrame({'rainfall_gauges': props['rainfall_gauges'].values[0],
                           'rain_id': props['rain_id'].values[0],
                           'distance': props['distance'].values[0]})
    gauges.index = range(len(gauges))
    
#    trigger = summary[summary.site_code == site_code]['alert'].values[0] > 0
    rain_stack_plot(site_code, gauges, offsetstart, start, end, tsn, threshold,
                    sc, output_path)