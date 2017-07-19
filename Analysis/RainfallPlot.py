##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ioff()

from datetime import timedelta
import pandas as pd
import numpy as np

import querySenslopeDb as q


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
        
def GetData(r, offsetstart, end):
    #raw data from senslope rain gauge
    rainfall = q.GetRawRainData(r, offsetstart)
    rainfall = rainfall.loc[rainfall['rain']>=0]
    rain_timecheck = rainfall
        
    #data resampled to 30mins
    blankdf = pd.DataFrame({'ts': [offsetstart, end], 'rain': [np.nan]*2})
    rainfall = rainfall.append(blankdf)
    rainfall = rainfall.drop_duplicates('ts')
    rainfall = rainfall.set_index('ts')
    if len(rain_timecheck) < 1:
        rainfall = rainfall.resample('30min',how='pad', label='right')
    else:
        rainfall = rainfall.resample('30min',how='sum', label='right')
        
    rainfall = rainfall[(rainfall.index>=offsetstart)]
    rainfall = rainfall[(rainfall.index<=end)]

    return rainfall

def shade_range(df, lst):
    lst.append((pd.to_datetime(df['ts'].values[0]), pd.to_datetime(df['ts'].values[0]) + timedelta(hours=0.5)))
    return lst

def plot_shade(df, ax):
    ax.axvspan(pd.to_datetime(df['shaded_range'].values[0][0]), pd.to_datetime(df['shaded_range'].values[0][1]), alpha = 0.5, color='#afeeee')

def PlotData(rain_gauge_col, offsetstart, start, end, sub, col, insax, cumax, fig, name):
    data = GetData(rain_gauge_col['rain_gauge'].values[0], offsetstart, end)
    
    #getting the rolling sum for the last24 hours
    rainfall2=pd.rolling_sum(data,48,min_periods=1)
    rainfall2=np.round(rainfall2,4)
    
    #getting the rolling sum for the last 3 days
    rainfall3=pd.rolling_sum(data,144,min_periods=1)
    rainfall3=np.round(rainfall3,4)

    data['24hr cumulative rainfall'] = rainfall2.rain
    data['72hr cumulative rainfall'] = rainfall3.rain
    data = data[(data.index>=start)]
    data = data[(data.index<=end)]
    
    data = data.reset_index()
    data = data.set_index('ts')
    if len(data) == len(data[pd.isnull(data).rain]):
        plot1 = data['rain'].fillna(0)
    else:
        plot1 = data['rain']
    plot2 = data['24hr cumulative rainfall']
    plot3 = data['72hr cumulative rainfall']
    plot4 = sub['half of 2yr max rainfall']
    plot5 = sub['2yr max rainfall']
    
    RG_num = col.loc[col.rain_gauge == rain_gauge_col['rain_gauge'].values[0]].index[0]
    inscurax = insax[RG_num]
    cumcurax = cumax[RG_num]
    
    try:
        nan_data = data[pd.isnull(data).rain]
        nan_data = nan_data.reset_index()
        nan_datadf = nan_data.groupby('ts')
        lst = []
        nan_range = nan_datadf.apply(shade_range, lst=lst)
        nan_range = nan_range[0]
        nan_range = nan_range[1:len(nan_range)]
        shaded_range = stitch_intervals(nan_range)
        shaded_df = pd.DataFrame({'shaded_range': shaded_range})
        shaded_grp = shaded_df.groupby('shaded_range')
        shaded_grp.apply(plot_shade, ax=inscurax)
    except:
        pass

    try:
        width = 0.01
        inscurax.bar(plot1.index,plot1,width,color='r') # instantaneous rainfall data
        cumcurax.plot(plot2.index,plot2,color='b') # 24-hr cumulative rainfall
        cumcurax.plot(plot3.index,plot3,color='r') # 72-hr cumulative rainfall #pink e377c2
        cumcurax.plot(plot4.index,plot4,color='b',linestyle='--') # half of 2-yr max rainfall
        cumcurax.plot(plot5.index,plot5,color='r',linestyle='--')  # 2-yr max rainfall
        b, t = cumcurax.get_ylim()
        if t > 500:
            t = 500
        cumcurax.set_ylim([b, t + 25])
        for tick in inscurax.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
        for tick in cumcurax.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
    except:
        pass
    
    cumcurax.set_xlim([sub.index[0], sub.index[-1]])
    ylabel = rain_gauge_col['rain_gauge'].values[0].replace('rain_noah_', 'NOAH') + ' (' + str(rain_gauge_col['d'].values[0]) + 'km)'
    ylabel = ylabel.replace(' (km)', '')
    inscurax.set_ylabel(ylabel, fontsize='medium')
    
    dfmt = md.DateFormatter('%m-%d')
    inscurax.xaxis.set_major_formatter(dfmt)
    cumcurax.xaxis.set_major_formatter(dfmt)
    
    fig.subplots_adjust(top=0.93, right=0.8, left=0.08, bottom=0.08, hspace=0.23, wspace=0.13)
    fig.suptitle(name+" as of "+str(end),fontsize='xx-large')

def SensorPlot(name, col, offsetstart, start, end, tsn, halfmax, twoyrmax, base, RainfallPlotsPath):
    
    ##INPUT:
    ##name; str; site name
    ##col; list; rain gauge table name
    ##end; datetime; end of rainfall data
    ##tsn; str; time format acceptable as file name
    ##halfmax; float; half of 2yr max rainfall, one-day cumulative rainfall threshold
    ##twoyrmax; float; 2yr max rainfall, three-day cumulative rainfall threshold    
    
    plt.xticks(rotation=70, size=5)       
    fig=plt.figure(figsize = (15,20))
    
    ins1 = fig.add_subplot(len(col),2,1)
    ins2 = fig.add_subplot(len(col),2,3, sharex=ins1, sharey=ins1)
    ins3 = fig.add_subplot(len(col),2,5, sharex=ins1, sharey=ins1)

    cum1 = fig.add_subplot(len(col),2,2)
    cum2 = fig.add_subplot(len(col),2,4, sharex=cum1)
    cum3 = fig.add_subplot(len(col),2,6, sharex=cum1)

    insax = [ins1, ins2, ins3]
    cumax = [cum1, cum2, cum3]

    if len(col) >= 4:
        ins4 = fig.add_subplot(len(col),2,7, sharex=ins1, sharey=ins1)
        cum4 = fig.add_subplot(len(col),2,8, sharex=cum1)
        insax.append(ins4)
        cumax.append(cum4)

    if len(col) == 5:
        ins5 = fig.add_subplot(len(col),2,9, sharex=ins1, sharey=ins1)
        cum5 = fig.add_subplot(len(col),2,10, sharex=cum1)
        insax.append(ins5)
        cumax.append(cum5)

    rain_gauge_col = col.groupby('rain_gauge')
    
    sub=base
    sub['half of 2yr max rainfall'] = halfmax  
    sub['2yr max rainfall'] = twoyrmax
    
    rain_gauge_col.apply(PlotData, offsetstart=offsetstart, start=start, end=end, sub=sub, col=col, insax=insax, cumax=cumax, fig=fig, name=name) 
    
    ins1.set_xlim([start - timedelta(hours=2), end + timedelta(hours=2)])
    cum1.set_xlim([start - timedelta(hours=2), end + timedelta(hours=2)])
    lgd = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='medium')
    plt.savefig(RainfallPlotsPath+'rainfall_'+tsn+'_'+name, dpi=100, 
        facecolor='w', edgecolor='w',orientation='landscape',mode='w',
        bbox_extra_artists=(lgd,))#, bbox_inches='tight')
    plt.close()

################################     MAIN     ################################

def main(siterainprops, offsetstart, start, end, tsn, s, output_path):

    ##INPUT:
    ##siterainprops; DataFrameGroupBy; contains rain noah ids of noah rain gauge near the site, one and three-day rainfall threshold
    
    ##OUTPUT:
    ##evaluates rainfall alert
    
    #rainfall properties from siterainprops
    name = siterainprops['name'].values[0]
    twoyrmax = siterainprops['max_rain_2year'].values[0]
    halfmax=twoyrmax/2
        
    rain_arq = siterainprops['rain_arq'].values[0]
    rain_senslope = siterainprops['rain_senslope'].values[0]
    RG1 = siterainprops['RG1'].values[0]
    RG2 = siterainprops['RG2'].values[0]
    RG3 = siterainprops['RG3'].values[0]

    index = [start, end]
    columns=['half of 2yr max rainfall','2yr max rainfall']
    base = pd.DataFrame(index=index, columns=columns)

    d_RG1 = siterainprops['d_RG1'].values[0]
    d_RG2 = siterainprops['d_RG2'].values[0]
    d_RG3 = siterainprops['d_RG3'].values[0]
    col = [rain_arq, rain_senslope, RG1, RG2, RG3]
    d = ['', '', d_RG1, d_RG2, d_RG3]
    col = pd.DataFrame({'rain_gauge': col, 'd': d})
    col = col.dropna()
    col.index = range(len(col))
    SensorPlot(name, col, offsetstart, start, end, tsn, halfmax, twoyrmax, base, output_path+s.io.RainfallPlotsPath)