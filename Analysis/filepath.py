import numpy as np
import os
import pandas as pd
from datetime import datetime, timedelta, time

import rtwindow as rtw
import querySenslopeDb as q

def output_file_path(site, plot_type, monitoring_end=False, positive_trigger=False, end=datetime.now()):
    
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

    window,config = rtw.getwindow(pd.to_datetime(end))
    
    # 3 most recent non-A0 public alert
    query = "SELECT * FROM senslopedb.site_level_alert"
    query += " WHERE site = '%s'" %site
    query += " AND source = 'public'"
    query += " AND (updateTS <= '%s'" %window.end
    query += "  OR (updateTS >= '%s'" %window.end
    query += "  AND timestamp <= '%s'))" %window.end
    query += " ORDER BY timestamp DESC LIMIT 4"
    
    public_alert = q.GetDBDataFrame(query)

    if plot_type == 'rainfall':
        monitoring_output_path = output_path + config.io.rainfallplotspath
    elif plot_type == 'subsurface':
        monitoring_output_path = output_path + config.io.subsurfaceplotspath
    elif plot_type == 'surficial':
        monitoring_output_path = output_path + config.io.surficialplotspath
    elif plot_type == 'trending_surficial':
        monitoring_output_path = output_path + config.io.trendingsurficialplotspath
    elif plot_type == 'eq':
        monitoring_output_path = output_path + config.io.eqplotspath
    else:
        monitoring_output_path = output_path + config.io.outputfilepath
        print 'unrecognized plot type; print to %s' %(monitoring_output_path)

    if positive_trigger and public_alert['alert'].values[0] == 'A0':
        event_path = output_path + config.io.outputfilepath + (site + window.end.strftime(' %d %b %Y') + '/').upper()

    elif (public_alert['alert'].values[0] == 'A0' and not monitoring_end) \
            or (not monitoring_end and public_alert['alert'].values[0] != 'A0' \
            and plot_type == 'rainfall' and window.end.time() not in [time(7, 30), time(19, 30)]):
        event_path = None

    else:
        public_alert = public_alert[public_alert.alert != 'A0']
        
        # one prev alert
        if len(public_alert) == 1:
            start_monitor = pd.to_datetime(public_alert['timestamp'].values[0])
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

        event_path = output_path + config.io.outputfilepath + \
                (site + start_monitor.strftime(' %d %b %Y') + '/').upper()

    for i in set([monitoring_output_path, event_path]) - set([None]):
        if not os.path.exists(str(i)):
            os.makedirs(str(i))

    file_path = {'event': event_path, 'monitoring_output': monitoring_output_path}

    return file_path