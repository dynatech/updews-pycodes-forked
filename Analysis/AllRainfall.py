import os
from datetime import datetime, timedelta, date, time
import numpy as np
import pandas as pd

import querySenslopeDb as q
import rainconfig as cfg
import RainfallAlert as RA
import RainfallPlot as RP

############################################################
##      TIME FUNCTIONS                                    ##    
############################################################

def get_rt_window(rt_window_length,roll_window_length,end=datetime.now()):
    
    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    
    ##OUTPUT: 
    ##end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

    ##set current time as endpoint of the interval
    end = pd.to_datetime(end)

    ##round down current time to the nearest HH:00 or HH:30 time value
    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    #starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+roll_window_length)
    
    return end, start, offsetstart

################################     MAIN     ################################

def main(site='', Print=True, end=datetime.now(), monitoring_end=False):

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    
    s = cfg.config()

    #creates directory if it doesn't exist
    if s.io.PrintPlot or s.io.PrintSummaryAlert:
        if not os.path.exists(output_path+s.io.RainfallPlotsPath):
            os.makedirs(output_path+s.io.RainfallPlotsPath)

    #1. setting monitoring window
    end, start, offsetstart = get_rt_window(s.io.rt_window_length,s.io.roll_window_length, end=end)
    tsn=end.strftime("%Y-%m-%d_%H-%M-%S")
    
    #rainprops containing noah id and threshold
    rainprops = q.GetRainProps('rain_props')  
    if site == '':
        pass
    else:
        rainprops = rainprops[rainprops.name == site]
    siterainprops = rainprops.groupby('name')
    
    siterainprops.apply(RA.main, end=end, s=s)
    
###############################################################################

if __name__ == "__main__":
    start_time = datetime.now()
    main()
    print "runtime = ", datetime.now()-start_time

