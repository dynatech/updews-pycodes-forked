from datetime import timedelta
import math
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.publicalerts as pub
import analysis.querydb as qdb
import dynadb.db as db
import gsm.gsmserver_dewsl3.sms_data as sms

def get_resampled_data(gauge_name, offsetstart, start, end, check_nd=True, is_realtime=True):
    """Resample retrieved data of gauge_name from offsetstart to end.
    
    Args:
        gauge_name (str): Rain gauge to retrieve data from.
        offsetstart (datetime): Start timestamp of data to be retrieved.
        start (datetime): Start timestamp of data to be analyzed.
        end (datetime): End timestamp of data to be retrieved.
        check_nd (bool): To check if data retrieved is empty. For empty data
                         retrieved: if set to True, returns empty dataframe; 
                         else, will return NaN containing dataframe.

    Returns:
        dataframe: Rainfall data of gauge_name from offsetstart to end.
    
    """
    
    if is_realtime:
        rainfall = qdb.get_raw_rain_data(gauge_name, from_time=offsetstart)
    else:
        rainfall = qdb.get_raw_rain_data(gauge_name, from_time=offsetstart, to_time=end)
    
    try:
        latest_ts = pd.to_datetime(rainfall['ts'].values[-1])
        time_checker = latest_ts <= end-timedelta(1)
    except:
        time_checker = True

    #returns blank dataframe if no data within the past hour
    if check_nd and time_checker:
        return pd.DataFrame()
    
    rainfall = rainfall[rainfall.rain >= 0 ]

    nan_replace = len(rainfall) == 0
    #add data to start and end of monitoring
    blankdf = pd.DataFrame({'ts': [end, offsetstart], 'rain': [np.nan, np.nan]})
    rainfall = rainfall.append(blankdf, sort=False)
    rainfall = rainfall.sort_values('ts')
    
    #data resampled to 30mins
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall.resample('30min', closed='right').sum(min_count=1)
    rainfall = rainfall[(rainfall.index >= offsetstart)]
    rainfall = rainfall[(rainfall.index <= end)]    
    
    if nan_replace:
        rainfall['rain'] = rainfall.rain.replace(0, np.nan)
    
    return rainfall
        
def get_unempty_rg_data(rain_props, offsetstart, start, end):
    """Retrieve data of nearest rain gauge with data within 24 hours.
    
    Args:
        rain_props (dataframe): Rain gauge available per site.
        offsetstart (datetime): Start timestamp of data to be retrieved.
        start (datetime): Start timestamp of data to be analyzed.
        end (datetime): End timestamp of data to be retrieved.

    Returns:
        tuple: data, gauge_name, rain_id
            data (dataframe): Rainfall data of nearest rain gauge with data.
            gauge_name (str): Nearest rain gauge with data.
            rain_id (int): ID of gauge_name.
            
    """

    for i in list(rain_props.index) + ['']:
        if i != '':
            gauge_name = rain_props[rain_props.index == i]['gauge_name'].values[0]
            rain_id = rain_props[rain_props.index == i]['rain_id'].values[0]
            data = get_resampled_data(gauge_name, offsetstart, start, end)
            if len(data) != 0:
                break
        else:
            data = pd.DataFrame({'ts': [end], 'rain': [np.nan]})
            data = data.set_index('ts')
            gauge_name = "No Alert! No ASTI/SENSLOPE Data"
            rain_id = "No Alert! No ASTI/SENSLOPE Data"

    return data, gauge_name, rain_id

def one_three_val_writer(rainfall, end):
    """Computes 1-day and 3-day cumulative rainfall at end.
    
    Args:
        rainfall (str): Data to compute cumulative rainfall from.
        end (datetime): End timestamp of alert to be computed.

    Returns:
        tuple: one, three
            one (float): 1-day cumulative rainfall.
            three (float): 3-day cumulative rainfall.
    
    """

    if len(rainfall.dropna()) == 0:
        return np.nan, np.nan
    
    one = rainfall[rainfall.index > end - timedelta(1)]['rain'].sum()
    three = rainfall[rainfall.index > end - timedelta(3)]['rain'].sum()
    
    return one,three
        
def summary_writer(site_id, site_code, gauge_name, rain_id, twoyrmax, halfmax,
                   rainfall, end, write_alert):
    """Summary of cumulative rainfall, threshold, alert and rain gauge used in
    analysis of rainfall.
    
    Args:
        site_id (int): ID per site.
        site_code (str): Three-letter code per site.
        gauge_name (str): Rain gauge used in rainfall analysis.
        rain_id (int): ID of gauge_name.
        twoyrmax (float): Threshold for 3-day cumulative rainfall per site.
        halfmax (float): Threshold for 1-day cumulative rainfall per site.
        rainfall (str): Data to compute cumulative rainfall from.
        end (datetime): End timestamp of alert to be computed.
        write_alert (bool): To write alert in database.

    Returns:
        dataframe: Summary of cumulative rainfall, threshold, alert and 
                   rain gauge used in analysis of rainfall.
    
    """

    one,three = one_three_val_writer(rainfall, end)

    #threshold is reached
    if one>=halfmax or three>=twoyrmax:
        ralert=1
    #no data
    elif one==None or math.isnan(one):
        ralert=-1
    #rainfall below threshold
    else:
        ralert=0

    if write_alert or ralert == 1:
        if qdb.does_table_exist('rainfall_alerts') == False:
            #Create a site_alerts table if it doesn't exist yet
            qdb.create_rainfall_alerts()

        columns = ['rain_alert', 'cumulative', 'threshold']
        alerts = pd.DataFrame(columns=columns)

        if ralert == 0:
            if one >= halfmax * 0.75:
                temp_df = pd.Series(['x', one, halfmax], index=columns)
            elif three >= twoyrmax * 0.75:
                temp_df = pd.Series(['x', three, twoyrmax], index=columns)
            else:
                temp_df = pd.Series([False, np.nan, np.nan], index=columns)
            
            alerts = alerts.append(temp_df, ignore_index=True, sort=False)
        else:
            if one >= halfmax:
                temp_df = pd.Series(['a', one, halfmax], index=columns)
                alerts = alerts.append(temp_df, ignore_index=True, sort=False)
            if three >= twoyrmax:
                temp_df = pd.Series(['b', three, twoyrmax], index=columns)
                alerts = alerts.append(temp_df, ignore_index=True, sort=False) 
            if ralert == -1:
                temp_df = pd.Series([False, np.nan, np.nan], index=columns)
                alerts = alerts.append(temp_df, ignore_index=True, sort=False)
                
        if alerts['rain_alert'][0] != False:
            for index, row in alerts.iterrows():
                rain_alert = row['rain_alert']
                cumulative = row['cumulative']
                threshold = row['threshold']
                if qdb.does_alert_exists(site_id, end, rain_alert).values[0][0] == 0:
                    df = pd.DataFrame({'ts': [end], 'site_id': [site_id],
                                       'rain_id': [rain_id], 'rain_alert': [rain_alert],
                                       'cumulative': [cumulative], 'threshold': [threshold]})
                    data_table = sms.DataTable('rainfall_alerts', df)
                    db.df_write(data_table)


    summary = pd.DataFrame({'site_id': [site_id], 'site_code': [site_code],
                        '1D cml': [one], 'half of 2yr max': [round(halfmax,2)],
                        '3D cml': [three], '2yr max': [round(twoyrmax,2)],
                        'DataSource': [gauge_name], 'rain_id': [rain_id],
                        'alert': [ralert]})
    
    return summary

def main(rain_props, end, sc, trigger_symbol, write_to_db=True):
    """Computes rainfall alert.
    
    Args:
        rain_props (dataframe): Contains rain gauges that can be used in 
                                rainfall analysis.
        end (datetime): Timestamp of alert to be computed.
        sc (dict): Server configuration.
        trigger_symbol: Alert symbol per alert level.

    Returns:
        dataframe: Summary of cumulative rainfall, threshold, alert and 
                   rain gauge used in analysis of rainfall.
    
    """

    #rainfall properties
    site_id = rain_props['site_id'].values[0]
    site_code = rain_props['site_code'].values[0]
    twoyrmax = rain_props['threshold_value'].values[0]
    halfmax = twoyrmax/2
    
    start = end - timedelta(float(sc['rainfall']['roll_window_length']))
    offsetstart = start - timedelta(hours=0.5)

    try:

        if qdb.get_alert_level(site_id, end)['alert_level'].values[0] > 0:
            
            start_monitor = pub.event_start(site_id, end)
            op_trig = pub.get_operational_trigger(site_id, start_monitor, end)
            op_trig = op_trig[op_trig.alert_level > 0]
            validity = max(op_trig['ts_updated'].values)
            validity = pub.release_time(pd.to_datetime(validity)) \
                                     + timedelta(1)
            if 3 in op_trig['alert_level'].values:
                validity += timedelta(1)
                
            if end + timedelta(hours=0.5) >= validity:
                write_alert = True
            else:
                write_alert = False            
        
        else:
            write_alert = False
    
    except:
        write_alert = False

    #data is gathered from nearest rain gauge
    rainfall, gauge_name, rain_id = get_unempty_rg_data(rain_props, offsetstart,
                                                        start, end)
    summary = summary_writer(site_id, site_code, gauge_name, rain_id, twoyrmax,
                             halfmax, rainfall, end, write_alert)

    operational_trigger = summary[['site_id', 'alert']]
    operational_trigger['alert'] = operational_trigger['alert'].map({-1:trigger_symbol[trigger_symbol.alert_level == -1]['trigger_sym_id'].values[0], 0:trigger_symbol[trigger_symbol.alert_level == 0]['trigger_sym_id'].values[0], 1:trigger_symbol[trigger_symbol.alert_level == 1]['trigger_sym_id'].values[0]})
    operational_trigger['ts'] = str(end)
    operational_trigger['ts_updated'] = str(end)
    operational_trigger = operational_trigger.rename(columns = {'alert': 'trigger_sym_id'})
    
    if write_to_db == True:
        qdb.alert_to_db(operational_trigger, 'operational_triggers')

    return summary
