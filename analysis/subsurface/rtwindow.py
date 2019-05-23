#rtwindow code

'''
DESC:
outputs class rtwindow with components:
int numpts : number of datapoints to check
datetime offsetstart : "earlier start" to compensate for rolling operations  
datetime start : start of monitoring data
datetime end : end of monitoring data
dataframe monwin : dataframe indexed from start to end at 30min intervals


USAGE:

import rtwindow as rtw

window_now = rtw.getWindow()

some_time = datetime(2016,12,25,07,00)
window_arbitrary = rtw.getWindow(some_time)

print window_now.monwin
print window_now.offsetstart
print window_now.start
print window_now.end
print window_now.numpts

'''
#    set current time as endpoint of the intervalUSA
from datetime import datetime, date, time, timedelta
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb


class RTWindow:
    def __init__(self, roll_window_numpts,offsetstart, start, end):
        self.numpts = roll_window_numpts
        self.offsetstart = offsetstart
        self.start = start
        self.end = end

def get_rt_window(rt_window_length,roll_window_size,num_roll_window_ops,endpt):
    
    ##DESCRIPTION:
    ##returns the time interval for real-time monitoring,

    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    ##roll_window_size; integer; number of data points to cover in moving window operations
    
    ##OUTPUT: 
    ##end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

    ##round down current time to the nearest HH:00 or HH:30 time value
    end_Year=endpt.year
    end_month=endpt.month
    end_day=endpt.day
    end_hour=endpt.hour
    end_minute=endpt.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    #starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+((num_roll_window_ops*roll_window_size-1)/48.))
    
    return end, start, offsetstart

def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops, endpt):
    
    ##DESCRIPTION:    
    ##returns number of data points per rolling window, endpoint of interval, starting point of interval, time interval for real-time monitoring, monitoring window dataframe
    
    ##INPUT:
    ##roll_window_length; float; length of rolling/moving window operations, in hours
    ##data_dt; float; time interval between data points, in hours    
    ##rt_window_length; float; length of real-time monitoring window, in days
    ##num_roll_window_ops
    
    ##OUTPUT:
    ##roll_window_numpts, end, start, offsetstart, monwin
    
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops, endpt)

    return roll_window_numpts, offsetstart, start, end

def get_window(end=datetime.now(),rt_window_length=''):
    
    sc = qdb.memcached()
    if rt_window_length == '':
        rt_window_length = float(sc['subsurface']['rt_window_length'])

    roll_window_numpts, offsetstart, start, end = set_monitoring_window(float(sc['subsurface']['roll_window_length']),float(sc['subsurface']['data_dt']),float(rt_window_length),float(sc['subsurface']['num_roll_window_ops']),endpt=end)
    
    return RTWindow(roll_window_numpts, offsetstart, start, end), sc
