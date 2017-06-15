from datetime import datetime, timedelta, date, time
import numpy as np
import os
import sys

import rainfallalert as ra
#import rainfall_plot as rp

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as qdb

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
    threshold = qdb.get_db_dataframe(query)
    return threshold

def rainfall_priorities(df):
    priorities = df.sort_values('distance')
    priorities = priorities[0:4]
    priorities['priority_id'] = range(1,5)
    return priorities

def rainfall_gauges(end=datetime.now()):
    query = "SELECT priority_id, site_id, rg.rain_id, gauge_name, data_source, distance "
    query += " FROM rainfall_priorities as rp"
    query += " left join rainfall_gauges as rg"
    query += " on rg.rain_id = rp.rain_id"
    query += " where date_activated <= '%s'" %end
    query += " and (date_deactivated >= '%s'" %end
    query += " or date_deactivated is null)"
    gauges = qdb.get_db_dataframe(query)
    gauges['gauge_name'] = np.array(','.join(gauges.data_source).replace('noah', 'rain_noah_').replace('senslope', 'rain_').split(','))+gauges.gauge_name
    site_gauges = gauges.groupby('site_id')
    priorities = site_gauges.apply(rainfall_priorities)
    return priorities.reset_index(drop=True)[['site_id', 'rain_id', 'gauge_name', 'priority_id']]

def site_threshold_gauges(threshold, gauges):
    threshold['rainfall_gauges'] = [gauges[gauges.site_id == threshold['site_id'].values[0]]['gauge_name'].values]
    threshold['rain_id'] = [gauges[gauges.site_id == threshold['site_id'].values[0]]['rain_id'].values]
    return threshold

def main(site_id='', Print=True, end=datetime.now()):
    start_time = datetime.now()
    print start_time

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    
    sc = qdb.memcached()

    #creates directory if it doesn't exist
    if (sc['rainfall']['print_plot'] or sc['rainfall']['print_summary_alert']) and Print:
        if not os.path.exists(output_path+sc['fileio']['rainfall_path']):
            os.makedirs(output_path+sc['fileio']['rainfall_path'])

    # setting monitoring window
    end, start, offsetstart = get_rt_window(float(sc['rainfall']['rt_window_length']),float(sc['rainfall']['roll_window_length']), end=end)
    tsn=end.strftime("%Y-%m-%d_%H-%M-%S")

    # 4 nearest rain gauges of each site
    gauges = rainfall_gauges()
    # threshold of each site
    threshold = rainfall_threshold()
    
    if site_id != '':
        threshold = threshold[threshold.site_id.isin(site_id)]
    site_threshold = threshold.groupby('site_id')
    
    query = "SELECT * FROM operational_trigger_symbols"
    query += " WHERE trigger_source = 'rainfall'"
    trigger_symbol = qdb.get_db_dataframe(query)
    
    gauges = site_threshold.apply(site_threshold_gauges, gauges=gauges)

    site_gauges = gauges.groupby('site_id')
    summary = site_gauges.apply(ra.main, end=end, sc=sc, trigger_symbol=trigger_symbol)
    summary = summary.reset_index(drop=True).set_index('site_id')[['1D cml', 'half of 2yr max', '3D cml', '2yr max', 'DataSource', 'alert', 'advisory']]

    if Print == True:
        if sc['rainfall']['print_summary_alert']:
            summary.to_csv(output_path+sc['fileio']['rainfall_path']+'SummaryOfRainfallAlertGenerationFor'+tsn+'.csv',sep=',',mode='w')
        
#        if s.rainfall.PrintPlot:
#            siterainprops.apply(rp.main, offsetstart=offsetstart, start=start, end=end, tsn=tsn, s=s, output_path=output_path)
        
    summary_json = summary.reset_index().to_json(orient="records")
    
    print "runtime = ", datetime.now()-start_time
    
    return summary_json

################################################################################

if __name__ == "__main__":
    main()