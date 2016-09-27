import os
from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from scipy.interpolate import splev, splrep, UnivariateSpline
from scipy.signal import gaussian
from scipy.ndimage import filters
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches
import Fukuzono
import sys

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path

from querySenslopeDb import *

def pick_point(event):
    point = event.artist
    xdata, ydata = point.get_data()
    ind = event.ind
    print "\nDate of Event: ", data[data.v == xdata[ind][0]].index.strftime("%m/%d/%Y %H:%M")[0]
    print "Velocity: ",xdata[ind][0]
    print "Acceleration: ",ydata[ind][0]

def GetGroundDF():
    try:

        query = 'SELECT timestamp, meas_type, site_id, crack_id, observer_name, meas, weather, reliability FROM gndmeas'
        
        df = GetDBDataFrame(query)
        return df
    except:
        raise ValueError('Could not get sensor list from database')


def replace_nin(x):
    if x == 'Messb':
        return 'mes'
    elif x == 'Nin':
        return 'mes'
    else:
        return x

def moving_average(series,sigma = 3):
    b = gaussian(39,sigma)
    average = filters.convolve1d(series,b/b.sum())
    var = filters.convolve1d(np.power(series-average,2),b/b.sum())
    return average,var


def remove_overlap(ranges):
    result = []
    current_start = -1
    current_stop = -1 

    for start, stop in sorted(ranges):
        if start > current_stop:
            # this segment starts after the last segment stops
            # just add a new segment
            result.append( (start, stop) )
            current_start, current_stop = start, stop
        else:
            # segments overlap, replace
            result[-1] = (current_start, stop)
            # current_start already guaranteed to be lower
            current_stop = max(current_stop, stop)

    return result

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

def intersect(ranges):
    result = []
    current_start = -1
    current_stop = -1 

    for start, stop in sorted(ranges):
        if start > current_stop:
            # this segment starts after the last segment stops
            # just add a new segment
            current_start, current_stop = start, stop
        else:
            # segments overlap, replace
            result.append((start,min(stop,current_stop)))
            # current_start already guaranteed to be lower
            current_stop = max(current_stop, stop)
            current_start = min(current_start,start)

    return result

def goodness_of_fit(x,y,reg):
    mean = np.mean(y)
    n = float(len(y))
    SS_tot = np.sum(np.power(y-mean,2))
    SS_res = np.sum(np.power(y-reg,2))
    coef_determination = 1 - SS_res/SS_tot
    RMSE = np.sqrt(SS_res/n)
    return SS_res,coef_determination,RMSE    

def resample_data(df,sample_size):
    df = df.resample(sample_size,base = 0,label = 'right',closed = 'right')
    df = df.interpolate()
    return df

###### PLOT PARAMETERS
tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

for i in range(len(tableau20)):    
    r, g, b = tableau20[i]    
    tableau20[i] = (r / 255., g / 255., b / 255.)



crack = 'A'
site = 'par'
size = 1
num_pts = 10
sample_size = '1H'

#spline constants
k = 3 #degree of spline
c = 1 #factor of error

#print directories
#for num_pts in [8,15,20]:
#    for c in [2,1,0.5,0.25]:
out_path = 'C:\Users\Win8\Documents\Dynaslope\\Data Analysis\\Filters\\Acceleration Velocity\\'
out_path = out_path + 'k {} Gaussian num_pts c {} {}\\{}\\{}\\'.format(k,c,num_pts,site,crack)
out_path1 = out_path + 'stats\\'
out_path2 = out_path + 'overall trend\\'
out_path3 = out_path + 'v vs a time evolution\\'

for paths in [out_path,out_path1,out_path2,out_path3]:
    if not os.path.exists(paths):
        os.makedirs(paths)

####getting data from database and allsites csv

#ONLINE METHOD
df = GetGroundDF()

#df2 = pd.read_csv('C:\Users\Win8\Documents\Dynaslope\plotter\\allsites.csv',parse_dates = ['timestamp'])
#df2['site_id'] = map(replace_nin,df2['site_id'])
#df2['meas'] = df2.meas.apply(lambda x: pd.to_numeric(x,errors = 'coerce'))
#df = df.append(df2)

##OFFLINE METHOD
#df = pd.read_csv('C:\Users\Win8\Documents\Dynaslope\\Data Analysis\\ground data.csv',parse_dates = ['timestamp'])

print df
#    df=df[df['meas']!=np.nan]
df = df[df['meas']<1000]
#    df=df[df['timestamp']!=' ']
df['timestamp'] = [d.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(d) else '' for d in df['timestamp']]
df=df[df['site_id']!=' ']
df=df[df['crack_id']!=np.nan]
df['timestamp']=pd.to_datetime(df['timestamp'])
df=df.dropna(subset=['meas'])
print np.unique(df['site_id'].values)

df['site_id'] = map(lambda x: x.lower(),df['site_id'])
df['crack_id'] = map(lambda x: x.title(),df['crack_id'])

data = df[df['site_id']==site]
data = data.set_index(['timestamp'])
data = data.loc[data.crack_id == crack,['meas']]

data.sort_index(inplace = True)

data['tvalue'] = data.index
data['delta'] = (data['tvalue']-data.index[0])

data = data.drop_duplicates(take_last = True)
data['t'] = data['delta'].apply(lambda x: x  / np.timedelta64(1,'D'))
data['x'] = data['meas']/100.
        
####AFTER DATA COLLECTION
t = np.array(data['t'].values-data['t'][0])
x = np.array(data['x'].values)
timestamp = np.array(data['tvalue'].values)
time_start = data['tvalue'][0]

v = np.array([])
a = np.array([])
v_0 = np.array([])
v_2 = np.array([])
v_3 = np.array([])
a_0 = np.array([])
a_2 = np.array([])
a_3 = np.array([])
old_alerts = []
new_alerts = []
legit_range = []
trend_alerts = []
all_vel = []

for i in np.arange(0,num_pts-1):
    v = np.append(v,np.nan)
    a = np.append(a,np.nan)
    old_alerts.append(np.nan)
    new_alerts.append(np.nan)
    trend_alerts.append(np.nan)
    all_vel.append(np.nan)

for i in np.arange(num_pts,len(t)+1):
#    if i != len(t)-20:
#        continue
    #data splicing    
    cur_t = t[i-num_pts:i]
    cur_x = x[i-num_pts:i]
    cur_timestamp = timestamp[i-num_pts:i]
    
    #data spline
    try:
        #Take the gaussian average of data points and its variance
        _,var = moving_average(cur_x)
        sp = UnivariateSpline(cur_t,cur_x,w=c/np.sqrt(var))
        t_n = np.linspace(cur_t[0],cur_t[-1],1000)
        
        #spline results    
        x_n = sp(t_n)
        v_n = sp.derivative(n=1)(t_n)
        a_n = sp.derivative(n=2)(t_n)
        
        #compute for velocity (cm/day) vs. acceleration (cm/day^2) in log axes
        x_s = sp(cur_t)
        v_s = abs(sp.derivative(n=1)(cur_t) * 100)
        a_s = abs(sp.derivative(n=2)(cur_t) * 100)
    except:
        print "Interpolation Error {}".format(pd.to_datetime(str(cur_timestamp[-1])).strftime("%m/%d/%Y %H:%M"))
        x_n = np.ones(len(t_n))*np.nan        
        v_n = np.ones(len(t_n))*np.nan
        a_n = np.ones(len(t_n))*np.nan
        x_s = np.ones(len(cur_t))*np.nan
        v_s = np.ones(len(cur_t))*np.nan
        a_s = np.ones(len(cur_t))*np.nan
    
    
    v = np.append(v,v_s[-1])
    a = np.append(a,a_s[-1])    
    
    #Goodness of fit
    SS_res,r2,RMSE = goodness_of_fit(cur_t,cur_x,x_s)
    text = 'SSE = {} \nR-square = {} \nRMSE = {}'.format(round(SS_res,4),round(r2,4),round(RMSE,4))
    #velocity alerts
    #Old Alerts
    time_delta = cur_t[-1] - cur_t[-2]
    abs_disp = abs(cur_x[-1] - cur_x[-2])*100
    #old alert table
    if time_delta >= 7:
        if abs_disp >= 75:
            crack_alert = 'L3'
            oa_color = tableau20[6]
            v_3 = np.append(v_3,v_s[-1])
            a_3 = np.append(a_3,a_s[-1])
        elif abs_disp >= 3:
            crack_alert = 'L2'
            oa_color = tableau20[16]
            v_2 = np.append(v_2,v_s[-1])
            a_2 = np.append(a_2,a_s[-1])
        else:
            crack_alert = 'L0'
            oa_color = tableau20[4]
            v_0 = np.append(v_0,v_s[-1])
            a_0 = np.append(a_0,a_s[-1])
    elif time_delta >= 3:
        if abs_disp >= 30:
            crack_alert = 'L3'
            oa_color = tableau20[6]
            v_3 = np.append(v_3,v_s[-1])
            a_3 = np.append(a_3,a_s[-1])            
        elif abs_disp >= 1.5:
            crack_alert = 'L2'
            oa_color = tableau20[16]
            v_2 = np.append(v_2,v_s[-1])
            a_2 = np.append(a_2,a_s[-1])            
        else:
            crack_alert = 'L0'
            oa_color = tableau20[4]
            v_0 = np.append(v_0,v_s[-1])
            a_0 = np.append(a_0,a_s[-1])
            
    elif time_delta >= 1:
        if abs_disp >= 10:
            crack_alert = 'L3'
            oa_color = tableau20[6]
            v_3 = np.append(v_3,v_s[-1])
            a_3 = np.append(a_3,a_s[-1])            
        elif abs_disp >= 0.5:
            crack_alert = 'L2'
            oa_color = tableau20[16]
            v_2 = np.append(v_2,v_s[-1])
            a_2 = np.append(a_2,a_s[-1])            
        else:
            crack_alert = 'L0'
            oa_color = tableau20[4]
            v_0 = np.append(v_0,v_s[-1])
            a_0 = np.append(a_0,a_s[-1])
            
    else:
        if abs_disp >= 5:
            crack_alert = 'L3'
            oa_color = tableau20[6]
            v_3 = np.append(v_3,v_s[-1])
            a_3 = np.append(a_3,a_s[-1])             
            
        elif abs_disp >= 0.5:
            crack_alert = 'L2'
            oa_color = tableau20[16]
            v_2 = np.append(v_2,v_s[-1])
            a_2 = np.append(a_2,a_s[-1])            
        else:
            crack_alert = 'L0'
            oa_color = tableau20[4]
            v_0 = np.append(v_0,v_s[-1])
            a_0 = np.append(a_0,a_s[-1])
    ##### Evaluate trending result
    #### Federico et. al Constants
    slope = 1.49905955613175
    intercept = -3.00263765777028
    t_crit = 4.53047399738543
    var_v_log = 215.515369339559
    v_log_mean = 2.232839766
    sum_res_square = 49.8880017417971
    n = 30.
    
    uncertainty = t_crit*np.sqrt(1/(n-2)*sum_res_square*(1/n + (np.log(v_s[-1]) - v_log_mean)**2/var_v_log))
    log_a = slope*np.log(v_s[-1]) + intercept
    log_a_upper = log_a + uncertainty
    log_a_lower = log_a - uncertainty
    a_upper = np.e**log_a_upper
    a_lower = np.e**log_a_lower
    
    if v_n[-1]*a_n[-1] < 0:
        trending_alert = 'Reject'
    else:
        if (a_s[-1] >= a_lower and a_s[-1] <= a_upper):
            trending_alert = 'Legit'
            if (crack_alert == 'L3' or crack_alert == 'L2'):
                legit_range.append((cur_t[-2],cur_t[-1]))
        else:
            trending_alert = 'Reject'
    
    trend_alerts.append(trending_alert)
    old_alert = crack_alert
    old_alerts.append(crack_alert)
    #New Alerts
    vel = abs_disp/time_delta
        #conversion from cm/day to cm/hr
    vel = vel/24.
    all_vel.append(vel)
    #new alert table
    if vel >= 1.8:
        crack_alert = 'L3'
        na_color = tableau20[6]
    elif vel >= 0.25:
        crack_alert = 'L2'
        na_color = tableau20[16]
    else:
        crack_alert = 'L0'
        na_color = tableau20[4]
    
    new_alert = crack_alert
    new_alerts.append(crack_alert)
    #plot position and vel, vs time
    
    fig, ax = plt.subplots(nrows = 3, ncols = 1, sharex = True)
    fig.set_size_inches(17,8)
    l1 = ax[0].plot(cur_t,cur_x,'.',color = tableau20[0],label = 'Data')
    l2 = ax[0].plot(t_n,x_n,color = tableau20[8], label = 'Interpolation')
    ax[0].set_ylabel('Disp (meters)')
    
    #boxes for alert label
    box1 = Rectangle((0, 0), 1, 1, fc=oa_color, fill=True, edgecolor= None, linewidth = 0, label = 'AlertO {}'.format(old_alert))
    box2 = Rectangle((0, 0), 1, 1, fc=na_color, fill=True, edgecolor=None, linewidth = 0, label = 'AlertN {}'.format(new_alert))
    
    lns = l1 + l2
    lns.append(box1)
    lns.append(box2)
    
    labs = [l.get_label() for l in lns]
    ax[0].grid()
    #printing of statistics
    props = dict(boxstyle = 'round',facecolor = 'white',alpha = 0.5)
    ax[0].text(1-0.150, 0.664705882353-0.30,text,transform = ax[0].transAxes,verticalalignment = 'top',horizontalalignment = 'left',bbox = props)    
  
    ax[0].legend(lns,labs,loc = 'upper left',fancybox = True,framealpha = 0.5)    
    
    ax[1].grid()
    ax[2].grid()
    l3 = ax[1].plot(t_n,v_n,color = tableau20[4],label = 'Velocity')
    ax[1].set_ylabel('Velocity (m/day)')
    
    
    l4 = ax[2].plot(t_n,a_n,color = tableau20[6],label = 'Acceleration')    
    ax[2].set_ylabel('Acceleration (m/day$^2$)')
    ax[2].set_xlabel('Time (days)')
    ax[2].legend(loc = 'upper left',fancybox = True,framealpha = 0.5)
    ax[1].legend(loc = 'upper left',fancybox = True,framealpha = 0.5)

    
    tsn = pd.to_datetime(str(cur_timestamp[-1]))
    tsn = tsn.strftime("%Y-%m-%d_%H-%M-%S")
    tsn2 = pd.to_datetime(str(cur_timestamp[-1])).strftime("%m/%d/%Y %H:%M")
    fig_out_path = out_path1 + tsn
    
  
#    statistics = AnchoredText(text,loc = 4)
#    statistics.set_alpha(0.5)
#    ax[0].add_artist(statistics)
    
    ax[0].set_title(tsn2+" "+site.upper()+" "+crack)
    plt.savefig(fig_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w')
    plt.close()
    
    if i - num_pts > 5 and i < len(t) - 5:
        fig2 = plt.figure()
        fig2.set_size_inches(15,8)
        ax2 = fig2.add_subplot(111)
        ax2.grid()
        ax2.plot(t[i-num_pts-5:i+5],x[i-num_pts-5:i+5],'.',color = tableau20[6],markersize = 10)
        ax2.plot(t[i-num_pts-5:i+5],x[i-num_pts-5:i+5],color = tableau20[0],label = 'Data')
        ax2.plot(t_n,x_n,color = tableau20[8],label = 'Interpolation',lw = 2)
        ax2.legend(loc = 'upper left')
        ax2.set_xlabel('Time (days)')
        ax2.set_ylabel('Disp (meters)')
        ax2.set_title(tsn2+" "+site.upper()+" "+crack+" Overall Trend")
        
        fig2_out_path = out_path2 + tsn
        plt.savefig(fig2_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w')
        
        plt.close()
    
data['v'] = v
data['a'] = a
data['old_alert'] = old_alerts
data['new_alert'] = new_alerts
data['trend_alert'] = trend_alerts
data['all_vel'] = all_vel
data.drop_duplicates(inplace = True)
v_min = min(np.concatenate((v_0,v_2,v_3)))
v_max = max(np.concatenate((v_0,v_2,v_3)))
v_theo = np.arange(v_min,v_max,0.0001)
a_theo, a_theo_up, a_theo_down = Fukuzono.fukuzono_constants(v_min = v_min, v_max = v_max, numpts = len(v_theo))

######### velocity vs acceleration time evolution

#data_0 = data[np.in1d(data.v,v_0)]
#data_2 = data[np.in1d(data.v,v_2)]
#data_3 = data[np.in1d(data.v,v_3)]

for i in np.arange(num_pts,len(t)+1):
    
    #Redundant Computation of Spline Results
    #Data Splicing
    cur_t = t[i-num_pts:i]
    cur_x = x[i-num_pts:i]
    cur_timestamp = timestamp[i-num_pts:i]

    #Data Spline
    try:
        _,var = moving_average(cur_x)
        sp = UnivariateSpline(cur_t,cur_x,w=c/np.sqrt(var))
        t_n = np.linspace(cur_t[0],cur_t[-1],1000)
        
        #Spline Results
        x_n = sp(t_n)
        v_n = sp.derivative(n=1)(t_n)
        a_n = sp.derivative(n=2)(t_n)
        x_s = sp(cur_t)
        
    except:
        print "Interpolation Error {}".format(pd.to_datetime(str(cur_timestamp[-1])).strftime("%m/%d/%Y %H:%M"))
        x_n = np.ones(len(t_n))*np.nan
        v_n = np.ones(len(t_n))*np.nan
        a_n = np.ones(len(t_n))*np.nan
        x_s = np.ones(len(cur_t))*np.nan
    
    #Goodness of Fit Computations
    SS_res,r2,RMSE = goodness_of_fit(cur_t,cur_x,x_s)
    text = 'SSE = {} \nR-square = {} \nRMSE = {}'.format(round(SS_res,4),round(r2,4),round(RMSE,4))

    #Velocity vs Acceleration Points
    cur_data = data[:cur_timestamp[-1]].tail(num_pts)
    cur_l0 = cur_data[np.in1d(cur_data.v,v_0)]
    cur_l2 = cur_data[np.in1d(cur_data.v,v_2)]
    cur_l3 = cur_data[np.in1d(cur_data.v,v_3)]
    
    cur_v,cur_a = cur_data.v.values,cur_data.a.values
    cur_v0,cur_a0 = cur_l0.v.values,cur_l0.a.values
    cur_v2,cur_a2 = cur_l2.v.values,cur_l2.a.values
    cur_v3,cur_a3 = cur_l3.v.values,cur_l3.a.values
    
    #Plot all data in a single figure
    
    #Velocity vs. Acceleration Time Evolution
    fig = plt.figure()
    fig.set_size_inches(15,8)
    ax1 = fig.add_subplot(121)
    ax1.get_xaxis().tick_bottom()    
    ax1.get_yaxis().tick_left()
    ax1.grid()
    ax1.fill_between(v_theo,a_theo_up,a_theo_down,facecolor = tableau20[1],alpha = 0.5)
    l1 = ax1.plot(v_theo,a_theo,c = tableau20[0],label = 'Fukuzono (1985)')
    ax1.plot(v_theo,a_theo_up,'--',c = tableau20[0])
    ax1.plot(v_theo,a_theo_down,'--', c = tableau20[0])
    ax1.plot(cur_v,cur_a,c = tableau20[10])
    l2 = ax1.plot(cur_v0,cur_a0,'o',c=tableau20[4],label = 'L0 Points',picker = 5)
    l3 = ax1.plot(cur_v2,cur_a2,'s',c = tableau20[16],label = 'L2 Points',picker = 5)
    l4 = ax1.plot(cur_v3,cur_a3,'^',c = tableau20[6],label = 'L3 Points',picker = 5)

    ax1.set_xlabel('Velocity (cm/day)')
    ax1.set_ylabel('Acceleration (cm/day$^2$)')
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    try:
        old_alert = data.loc[pd.to_datetime(cur_timestamp[-1])].old_alert
        new_alert = data.loc[pd.to_datetime(cur_timestamp[-1])].new_alert
        if old_alert == 'L3': old_alert = data.loc[pd.to_datetime(cur_timestamp[-1])].old_alert
    except:
        old_alert = data.loc[pd.to_datetime(cur_timestamp[-1])].old_alert
        new_alert = data.loc[pd.to_datetime(cur_timestamp[-1])].new_alert
        old_alert = old_alert.drop_duplicates().values[0]
        new_alert = new_alert.drop_duplicates().values[0]
    
    if old_alert == 'L3':
        oa_color = tableau20[6]
    elif old_alert == 'L2':
        oa_color = tableau20[16]
    else:
        oa_color = tableau20[4]
    
    if new_alert == 'L3':
        na_color = tableau20[6]
    elif new_alert == 'L2':
        na_color = tableau20[16]
    else:
        na_color = tableau20[4]
    
    
    box1 = Rectangle((0, 0), 1, 1, fc=oa_color, fill=True, edgecolor= None, linewidth = 0, label = 'AlertO {}'.format(old_alert))
    box2 = Rectangle((0, 0), 1, 1, fc=na_color, fill=True, edgecolor=None, linewidth = 0, label = 'AlertN {}'.format(new_alert))
    
    lns = l1 + l2 + l3 + l4
    lns.append(box1)
    lns.append(box2)
    
    labs = [l.get_label() for l in lns]
    
    ax1.legend(lns,labs,loc = 'upper left',fancybox = True, framealpha = 0.5)
    
    tsn3 = pd.to_datetime(str(cur_timestamp[-1])).strftime("%b %d, %Y %H:%M")
    
    tsn4 = pd.to_datetime(str(cur_timestamp[-1]))
    tsn4 = tsn4.strftime("%Y-%m-%d_%H-%M-%S")    
    
    fig.suptitle(site.upper() + " Crack " + crack + " Velocity and Acceleration" + " {}".format(tsn3))    
    
    #Plot Displacement vs. Time with interpolation curvewe2
    ax2 = fig.add_subplot(222)
    ax2.grid()
    ax2.plot(cur_t,cur_x,'.',color = tableau20[0],label = 'Data')
    ax2.plot(t_n,x_n,color = tableau20[8],label = 'Interpolation')
    ax2.set_ylabel('Disp (meters)')
    props = dict(boxstyle = 'round',facecolor = 'white',alpha = 0.5)
    ax2.text(1-0.320, 0.664705882353-0.43,text,transform = ax2.transAxes,verticalalignment = 'top',horizontalalignment = 'left',bbox = props)        
    ax2.legend(loc = 'upper left',fancybox = True, framealpha = 0.5)
    
    #FIX THIS SHIT!!!!
    #Plot Velocity vs. Time    
    ax3 = fig.add_subplot(224,sharex = ax2)
    ax3.grid()
    ax3.plot(t_n,v_n,c = tableau20[4],label = 'Velocity')
    ax3.set_ylabel('Velocity (m/day)')
    ax3.set_xlabel('Time (days)')    
    ax3.legend(loc = 'upper left',fancybox = True, framealpha = 0.5)
    
#    #Plot Acceleration vs. Time
    ax4 = ax3.twinx()
    ax4.plot(t_n,a_n,c = tableau20[6],label = 'Acceleration')
    ax4.set_ylabel('Acceleration (m/day$^2$)')
    ax4.legend(loc = 'upper right',fancybox = True, framealpha = 0.5)
#    
#    #Plot adjustments and saving
    fig4_out_path = out_path3 + " {} ".format(tsn4) + site + " " + crack + " velocity vs acceleration" +" {} {}".format(str(k),str(c).replace('.',''))
    
    plt.subplots_adjust(left = 0.09, right = 0.87, wspace = 0.20)
    plt.savefig(fig4_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches = 'tight')
    plt.close()    
    
###plot velocity vs. acceleration in log axes
fig = plt.figure()
fig.set_size_inches(10,8)
ax = fig.add_subplot(111)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.get_xaxis().tick_bottom()    
ax.get_yaxis().tick_left() 
ax.grid()
ax.fill_between(v_theo,a_theo_up,a_theo_down,facecolor = tableau20[1],alpha = 0.5)
ax.plot(v_theo,a_theo,c=tableau20[0],label = 'Fukuzono (1985)')
ax.plot(v_theo,a_theo_up,'--',c=tableau20[0])
ax.plot(v_theo,a_theo_down,'--',c=tableau20[0])
ax.plot(v_0,a_0,'o',c=tableau20[4],label = 'L0 Points',picker = 5)
ax.plot(v_2,a_2,'s',c = tableau20[16],label = 'L2 Points',picker = 5)
ax.plot(v_3,a_3,'^',c = tableau20[6],label = 'L3 Points',picker = 5)
ax.set_xlabel('Velocity (cm/day)',fontsize = 15)
ax.set_ylabel('Acceleration (cm/day$^2$)',fontsize = 15)
ax.set_xscale('log')
ax.set_yscale('log')
ax.legend(loc = 'upper left',fancybox = True)
ax.set_title(site.upper() + " Crack " + crack + " Velocity vs. Acceleration",fontsize = 18)
fig.canvas.mpl_connect('pick_event',pick_point)

fig3_out_path = out_path + " " + site + " " + crack + " velocity vs acceleration" +" {} {}".format(str(k),str(c).replace('.',''))
plt.savefig(fig3_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches='tight')

##### Plot crack displacement vs. time
dates = data.index
t_l0 = data[data.old_alert == 'L0'].t.values
t_l2 = data[data.old_alert == 'L2'].t.values
t_l3 = data[data.old_alert == 'L3'].t.values
x_l0 = data[data.old_alert == 'L0'].x.values
x_l2 = data[data.old_alert == 'L2'].x.values
x_l3 = data[data.old_alert == 'L3'].x.values
all_vel = data.all_vel.values
    
new_l2_ranges = stitch_intervals(zip(data.shift()[data.new_alert == 'L2'].t.values,data[data.new_alert == 'L2'].t.values))
new_l3_ranges = stitch_intervals(zip(data.shift()[data.new_alert == 'L3'].t.values,data[data.new_alert == 'L3'].t.values))

legit_ranges = stitch_intervals(legit_range)


fig = plt.figure()
fig.set_size_inches(15,8)
ax = fig.add_subplot(211)
ax.grid()
ax.plot(t,x*100,'-',c = tableau20[0])
l1 = ax.plot(t_l0,x_l0*100,'o',c = tableau20[4],label = 'L0 disp')
l2 = ax.plot(t_l2,x_l2*100,'s',c = tableau20[16],label = 'L2 disp')
l3 = ax.plot(t_l3,x_l3*100,'^',c=tableau20[6],label = 'L3 disp')
patch_l2 = mpatches.Patch(color = tableau20[2], label = 'New L2')
patch_l3 = mpatches.Patch(color = tableau20[6], label = 'New L3')
patch_legit = mpatches.Patch(color = tableau20[12], label = 'Trending')


ax.set_ylabel('Displacement (cm)')
ax.set_ylim(top = max(x)+0.01,bottom = min(x)-0.01)

lns = l1 + l2 + l3
lns.append(patch_legit)

labs = [l.get_label() for l in lns]

ax.legend(lns,labs, loc = 'upper left',fancybox = True)

ax2 = fig.add_subplot(212,sharex = ax)
ax2.grid()
ax2.plot(t,all_vel,'-',c = tableau20[12])
l1 = ax2.plot(t,all_vel,'o',c = tableau20[10],label = 'Crack Velocity')
all_time = np.arange(min(t),max(t),0.001)
l2 = ax2.plot(all_time,1.8*np.ones(len(all_time)),'--',c = tableau20[6],label = 'New L3')
l3 = ax2.plot(all_time,0.25*np.ones(len(all_time)),'--',c = tableau20[16],label = 'New L2')
for i,j in legit_ranges:
    ax2.axvspan(i,j,facecolor = tableau20[12],ec = None, alpha = 0.5)
    ax.axvspan(i,j,facecolor = tableau20[12],ec = None, alpha = 0.5)

lns = l1 + l2 + l3
lns.append(patch_legit)
labs = [l.get_label() for l in lns]

ax2.legend(lns,labs,loc = 'upper right', fancybox = True,framealpha = 0.5)
ax2.set_xlabel('Time (days)')
ax2.set_ylabel('Velocity (cm/hr)')




fig.suptitle(site.upper()+" Crack " + crack + " Displacement vs. Time with Old and New Alerts")
fig5_out_path = out_path + " {} Crack {} Displacement vs Time ".format(site.upper(),crack)
plt.savefig(fig5_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches = 'tight')

    
#tck = splrep(t,x,k=2,s=0)
#
#t_new = np.arange(0,max(t),0.001)
#x_new = splev(t_new,tck,der = 0)
#
#plt.plot(t,x,'.')
#plt.plot(t_new,x_new)