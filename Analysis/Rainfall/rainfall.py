from datetime import datetime, timedelta, date, time
import numpy as np
import os
import sys

import rainconfig as cfg
import rainfall_alert as ra
#import rainfall_plot as rp

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

############################################################
##      TIME FUNCTIONS                                    ##    
############################################################

def get_rt_window(rt_window_length, roll_window_length, end=datetime.now()):
    
    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    
    ##OUTPUT: 
    ##end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

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

def rainfall_threshold(threshold_name='two_year_max'):
    query = "SELECT site_id, threshold_value FROM rainfall_thresholds where threshold_name = '%s'" %threshold_name
    threshold = q.GetDBDataFrame(query)
    return threshold

def rainfall_priorities(df):
    priorities = df.sort_values('distance')
    priorities = priorities[0:4]
    priorities['priority_id'] = range(1,5)
    return priorities

def rainfall_gauges():
    query = "SELECT priority_id, site_id, rg.rain_id, gauge_name, data_source, distance FROM rainfall_priorities as rp left join rainfall_gauges as rg on rp.rain_id = rg.rain_id"
    gauges = q.GetDBDataFrame(query)
    gauges['gauge_name'] = np.array(','.join(gauges.data_source).replace('noah', 'rain_noah_').replace('senslope', 'rain_').split(','))+gauges.gauge_name
    site_gauges = gauges.groupby('site_id')
    priorities = site_gauges.apply(rainfall_priorities)
    return priorities.reset_index(drop=True)[['site_id', 'rain_id', 'gauge_name', 'priority_id']]

def site_threshold_gauges(threshold, gauges):
    threshold['rainfall_gauges'] = [gauges[gauges.site_id == threshold['site_id'].values[0]]['gauge_name'].values]
    threshold['rain_id'] = [gauges[gauges.site_id == threshold['site_id'].values[0]]['rain_id'].values]
    return threshold

def main(site_id='', Print=True, end=datetime.now()):

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    
    s = cfg.config()

    #creates directory if it doesn't exist
    if (s.io.PrintPlot or s.io.PrintSummaryAlert) and Print:
        if not os.path.exists(output_path+s.io.RainfallPlotsPath):
            os.makedirs(output_path+s.io.RainfallPlotsPath)

    # setting monitoring window
    end, start, offsetstart = get_rt_window(s.io.rt_window_length,s.io.roll_window_length, end=end)
    tsn=end.strftime("%Y-%m-%d_%H-%M-%S")

    # 4 nearest rain gauges of each site
    gauges = rainfall_gauges()
    # threshold of each site
    threshold = rainfall_threshold()
    
    if site_id != '':
        threshold = threshold[threshold.site_id.isin(site_id)]
    site_threshold = threshold.groupby('site_id')
    
    gauges = site_threshold.apply(site_threshold_gauges, gauges=gauges)

    site_gauges = gauges.groupby('site_id')
    summary = site_gauges.apply(ra.main, end=end, s=s)
    summary = summary.reset_index(drop=True).set_index('site_id')[['1D cml', 'half of 2yr max', '3D cml', '2yr max', 'DataSource', 'alert', 'advisory']]

    if Print == True:
        if s.io.PrintSummaryAlert:
            summary.to_csv(output_path+s.io.RainfallPlotsPath+'SummaryOfRainfallAlertGenerationFor'+tsn+s.io.CSVFormat,sep=',',mode='w')
        
#        if s.io.PrintPlot:
#            siterainprops.apply(rp.main, offsetstart=offsetstart, start=start, end=end, tsn=tsn, s=s, output_path=output_path)
        
    summary_json = summary.reset_index().to_json(orient="records")
    
    return summary_json

################################################################################

if __name__ == "__main__":
    start_time = datetime.now()
    main()
    print "runtime = ", datetime.now()-start_time

