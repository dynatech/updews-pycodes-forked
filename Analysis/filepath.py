import os
import pandas as pd
from datetime import datetime, timedelta, time

import rtwindow as rtw
import querySenslopeDb as q

def output_file_path(site, plot_type, monitoring_end=False, initial_trigger=False, end=datetime.now()):
    
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

    window,config = rtw.getwindow(pd.to_datetime(end))
    
    # 3 most recent non-A0 public alert
    query = "SELECT * FROM senslopedb.site_level_alert"
    query += " WHERE site = '%s'" %site
    query += " AND source = 'public'"
    query += " AND alert != 'A0'"
    query += " AND (updateTS <= '%s'" %window.end
    query += "  OR (updateTS >= '%s'" %window.end
    query += "  AND timestamp <= '%s'))" %window.end
    query += " ORDER BY timestamp DESC LIMIT 3"
    
    public_alert = q.GetDBDataFrame(query)

    if initial_trigger:
        path = config.io.outputfilepath + (site + window.end.strftime(' %d %b %Y')).upper()

    elif not monitoring_end and (pd.to_datetime(public_alert['updateTS'].values[0]) \
            < window.end - timedelta(hours=0.5) or (public_alert['alert'].values[0] != 'A0' \
            and plot_type == 'rainfall' and window.end.time() not in [time(7, 30), time(19, 30)])):
        if plot_type == 'rainfall':
            path = config.io.rainfallplotspath
        elif plot_type == 'subsurface':
            path = config.io.subsurfaceplotspath
        elif plot_type == 'surficial':
            path = config.io.surficialplotspath
        elif plot_type == 'trending_surficial':
            path = config.io.trendingsurficialplotspath
        else:
            print 'unrecognized plot type; print to %s' %(output_path + config.io.outputfilepath)
            return

    else:
        public_alert = public_alert[public_alert.alert != 'A0']
        
        # one prev alert
        if len(public_alert) == 1:
            start_monitor = public_alert['timestamp'].values[0]
        # two prev alert
        elif len(public_alert) == 2:
            # one event with two prev alert
            if pd.to_datetime(public_alert['timestamp'].values[0]) - pd.to_datetime(public_alert['updateTS'].values[1]) <= timedelta(hours=0.5):
                start_monitor = pd.to_datetime(public_alert['timestamp'].values[1])
            else:
                start_monitor = pd.to_datetime(public_alert['timestamp'].values[0])
        # three prev alert
        else:
            if pd.to_datetime(public_alert['timestamp'].values[0]) - pd.to_datetime(public_alert['updateTS'].values[1]) <= timedelta(hours=0.5):
                # one event with three prev alert
                if pd.to_datetime(public_alert['timestamp'].values[1]) - pd.to_datetime(public_alert['updateTS'].values[2]) <= timedelta(hours=0.5):
                    start_monitor = pd.to_datetime(public_alert.timestamp.values[2])
                # one event with two prev alert
                else:
                    start_monitor = pd.to_datetime(public_alert['timestamp'].values[1])
            else:
                start_monitor = pd.to_datetime(public_alert['timestamp'].values[0])

        path = config.io.outputfilepath + (site + start_monitor.strftime(' %d %b %Y')).upper()

    if not os.path.exists(output_path+path):
        os.makedirs(output_path+path)

    return output_path + path