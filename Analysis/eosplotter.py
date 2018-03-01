##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()

from datetime import datetime, timedelta, time, date
import os
import pandas as pd
import sys

import AllRainfall as rain
import ColumnPlotter as plotter
import genproc as gen
import querySenslopeDb as qdb
import rtwindow as rtw

#include the path of outer folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'GroundAlert/'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import GroundDataAlertLibWithTrendingNewAlerts as surf_plot


def round_data_time(date_time):
    
    date_time = pd.to_datetime(date_time)
    date_year = date_time.year
    date_month = date_time.month
    date_day = date_time.day
    time_hour = date_time.hour
    time_minute = date_time.minute
    if time_minute < 30:
        time_minute = 0
    else:
        time_minute = 30
    date_time = datetime.combine(date(date_year, date_month, date_day),
                           time(time_hour, time_minute,0))

    return date_time

def round_shift_time(date_time):
    
    if date_time.time() > time(7, 30) and date_time.time() <= time(19, 30):
        shift_time = time(8, 0)
    else:
        shift_time = time(20, 0)
        date_time -= timedelta(1)
    date_time = datetime.combine(date_time.date(), shift_time)

    return date_time

def tsm_plot(tsm_name, end, shift_datetime):
    
    query = "SELECT max(timestamp) AS ts FROM %s" %tsm_name
    
    try:
        ts = pd.to_datetime(qdb.GetDBDataFrame(query)['ts'].values[0])
        if ts < shift_datetime:
            return
    except:
        return
    
    if ts > end:
        ts = end
    
    window, config = rtw.getwindow(ts)
    col = qdb.GetSensorList(tsm_name)
    monitoring = gen.genproc(col[0], window, config,
                             fixpoint=config.io.column_fix)
    plotter.main(monitoring, window, config, realtime=False,
                 non_event_path=False)

def subsurface(site, end, shift_datetime):
    sensor_site = site[0:3] + '%'
    query = "SELECT * FROM site_column_props where name LIKE '%s'" %sensor_site
    df = qdb.GetDBDataFrame(query)
    tsm_set = set(df['name'].values)
    for tsm_name in tsm_set:
        tsm_plot(tsm_name, end, shift_datetime)
    
def surficial(site, end, shift_datetime):

    site_query = "( site_id = '%s' " %site
    if site == 'bto':
        site_query += "or site_id = 'bat' )"
    elif site == 'mng':
        site_query += "or site_id = 'man' )"
    elif site == 'png':
        site_query += "or site_id = 'pan' )"
    elif site == 'jor':
        site_query += "or site_id = 'pob' )"
    elif site == 'tga':
        site_query += "or site_id = 'tag' )"
    else:
        site_query += ')'

    query =  "SELECT max(timestamp) AS ts FROM gndmeas "
    query += "WHERE %s " %site_query
    query += "AND timestamp >= '%s' " %shift_datetime
    query += "AND timestamp <= '%s' " %end

    ts = qdb.GetDBDataFrame(query)['ts'].values[0]

    if ts != None:
        surf_plot.PlotForEvent(site,ts)

def site_plot(public_alert, end, shift_datetime):
    
    if end.time() not in [time(7, 30), time(19, 30)]:
        if public_alert['alert'].values[0] != 'A0' or public_alert['alert'].values[1] == 'A0':
            return

    site = public_alert['site'].values[0]
    
    subsurface(site, end, shift_datetime)
    surficial(site, end, shift_datetime)
    rain.main(site=site, end=end, alert_eval=False, plot=True, realtime=False)

def main(end=''):
    
    start = datetime.now()
    
    if end == '':
        try:
            end = pd.to_datetime(sys.argv[1])
            if end > start + timedelta(hours=0.5):
                print 'invalid timestamp'
                return
        except:
            end = datetime.now()
    else:
        end = pd.to_datetime(end)

    end = round_data_time(end)
    shift_datetime = round_shift_time(end)

    if end.time() not in [time(3, 30), time(7, 30), time(11, 30), time(15, 30),
               time(19, 30), time(23, 30)]:
        return

    query =  "SELECT * FROM site_level_alert "
    query += "WHERE source = 'public' "
    query += "AND ((updateTS >= '%s' " %(end - timedelta(hours=0.5))
    query += "  AND timestamp <= '%s' " %end
    query += "  AND alert REGEXP '1|2|3') "
    query += "OR (timestamp = '%s' " %end
    query += "  AND alert = 'A0')) "
    query += "ORDER BY timestamp DESC"
    public_alert = qdb.GetDBDataFrame(query)

    if len(public_alert) != 0:
        rain.main(site='', end=end, Print=True, db_write=False)
        site_public_alert = public_alert.groupby('site', as_index=False)
        site_public_alert.apply(site_plot, end=end,
                                shift_datetime=shift_datetime)


################################################################################

if __name__ == "__main__":
    main()