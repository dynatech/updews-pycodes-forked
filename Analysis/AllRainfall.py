import os
from datetime import datetime, timedelta, date, time
import numpy as np
import pandas as pd

import filepath
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

def main(site='', end=datetime.now(), Print=False, alert_eval=True,
         plot=False, monitoring_end=True, positive_trigger=True):
    
    start_time = datetime.now()
    print start_time
    
    output_path = filepath.output_file_path('all', 'rainfall')['monitoring_output']
    
    s = cfg.config()

    #1. setting monitoring window
    end, start, offsetstart = get_rt_window(s.io.rt_window_length,
                                            s.io.roll_window_length, end=end)
    tsn=end.strftime("%Y-%m-%d_%H-%M-%S")
    
    #rainprops containing noah id and threshold
    rainprops = q.GetRainProps('rain_props')  
    if site == '':
        pass
    else:
        rainprops = rainprops[rainprops.name == site]
    siterainprops = rainprops.groupby('name')

    if alert_eval:
        summary = siterainprops.apply(RA.main, end=end, s=s)
        summary = summary.reset_index(drop=True).set_index('site')
        summary = summary[['1D cml', 'half of 2yr max', '3D cml', '2yr max',
                           'DataSource', 'alert', 'advisory']]	
        summary[['1D cml', 'half of 2yr max', '3D cml', '2yr max']] = \
               np.round(summary[['1D cml', 'half of 2yr max', '3D cml',
                                 '2yr max']], 1)	
        summary_json = summary.reset_index()	
        summary_json['ts'] = str(end)	
        summary_json = summary_json.to_json(orient="records")	
	
        if Print:
            summary.to_csv(output_path + 'SummaryOfRainfallAlertGenerationFor' \
                           + tsn+s.io.CSVFormat, sep=',', mode='w')
    else:
        summary_json = [{}]
	
    if plot:
        siterainprops.apply(RP.main, offsetstart=offsetstart, start=start,
                            end=end, tsn=tsn, s=s,
                            monitoring_end=monitoring_end,
                            positive_trigger=positive_trigger)	
    
    print "runtime = ", datetime.now()-start_time
                                    
    return summary_json
    
###############################################################################

if __name__ == "__main__":
    main()

