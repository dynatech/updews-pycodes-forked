import numpy as np
import os
import pandas as pd
import random
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.earthquake.eqalert as eq
import analysis.querydb as qdb
import analysis.rainfall.rainfall as rain
import analysis.surficial.markeralerts as surf
import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import volatile.memory as mem


def rainfall(site_id, ts, rain_id, rain_alert=None):
    """Insert values to rainfall_alerts and operational_triggers tables 
    to (re)trigger rainfall alert
    
    Args:
        site_id (int): ID of site to compute rainfall analysis for.
        ts (datetime): Timestamp of alert trigger.
        rain_id (int): ID of rain gauge to use as data source.
        rain_alert (str: {'a', 'b', 'x', None}, default None): Type of rainfall alert. 
                a: 1-day cumulative rainfall threshold  exceeded
                b: 3-day cumulative rainfall threshold  exceeded
                x: 
                None: both 1-day and 3-day cumulative rainfall threshold  exceeded    
    """

    # 4 nearest rain gauges of each site with threshold and distance from site
    gauges = rain.rainfall_gauges()
    df = gauges.loc[(gauges.site_id == site_id) & (gauges.rain_id == rain_id), ['site_id', 'rain_id', 'threshold_value']]
    df = df.rename(columns={'threshold_value': 'threshold'})
    df.loc[:, 'ts'] = ts
    # rainfall cumulative based on alert level
    if rain_alert != 'x':
        df.loc[:, 'rain_alert'] = 'b'
        df.loc[:, 'cumulative'] = 1.2 * df.loc[:, 'threshold']
    else:
        df.loc[:, 'rain_alert'] = 'x'
        df.loc[:, 'cumulative'] = 0.76 * df.loc[:, 'threshold']
    if rain_alert == 'a' or rain_alert == None:
        dfa = df.copy()
        dfa.loc[:, ['cumulative', 'threshold']] = dfa.loc[:, ['cumulative', 'threshold']].div(2)
        dfa.loc[:, 'rain_alert'] = 'a'
        if rain_alert == 'a':
            df = dfa.copy()
        else:
            df = df.append(dfa, ignore_index=True)
    qdb.write_rain_alert(df)

    # writes to operational_triggers
    trigger_symbol = mem.get('df_trigger_symbols')
    if rain_alert == 'x':
        alert_level = 0
    else:
        alert_level = 1
    trigger_sym_id = trigger_symbol.loc[(trigger_symbol.trigger_source == 'rainfall') & (trigger_symbol.alert_level == alert_level), 'trigger_sym_id'].values[0]
    operational_trigger = pd.DataFrame({'site_id': [site_id], 'trigger_sym_id': [trigger_sym_id], 'ts': [ts], 'ts_updated': [ts]})
    qdb.alert_to_db(operational_trigger, 'operational_triggers')

    return df


def surficial(site_id, ts, alert_level):
    """Insert values to marker_observations, marker_data, marker_alerts, and 
    operational_triggers to (re)trigger surficial alert.
    
    Args:
        site_id (int): ID of site to compute surficial analysis for.
        ts (datetime): Timestamp of alert trigger.
        alert_level (int: {0, 1, 2, 3}, default None): Surficial alert level.
    """

    # get last data for site_id
    conn = mem.get('DICT_DB_CONNECTIONS')
    query = "SELECT ts, marker_id, marker_name, measurement "
    query += "FROM {analysis}.marker_observations "
    query += "INNER JOIN {common}.sites USING (site_id) "
    query += "INNER JOIN {analysis}.marker_data using (mo_id) "
    query += "INNER JOIN (SELECT data_id, displacement, time_delta, alert_level, processed FROM {analysis}.marker_alerts) sub1 USING (data_id) "
    query += "INNER JOIN (SELECT marker_id, marker_name FROM {analysis}.view_marker_history) sub2 USING (marker_id) "
    query += "WHERE site_id = {site_id} "
    query += "AND ts IN ( "
    query += "  SELECT MAX(ts) FROM {analysis}.marker_observations "
    query += "  WHERE ts < '{ts}' "
    query += "    AND site_id = {site_id})"
    query = query.format(analysis=conn['analysis']['schema'], common=conn['common']['schema'], site_id=site_id, ts=ts)
    df = db.df_read(query, resource='sensor_analysis')
        
    # compute diff in measurements to reach threshold
    if alert_level == 3:
        rate = 1.8
    elif alert_level in (1,2):
        rate = 0.25
    else:
        rate = 0
    meas_diff = np.ceil(rate * (ts-df.ts[0]).total_seconds()/3600)

    # input measurements in inbox
    gndmeas = df.loc[:, ['marker_id', 'marker_name', 'measurement']]
    gndmeas.loc[:, 'ts'] = ts
    gndmeas.loc[:, 'measurement'] += meas_diff
    if alert_level == 1:
        temp_gndmeas = gndmeas.copy()
        temp_gndmeas.loc[:, 'ts'] -= (ts - df.ts[0])/2
        temp_gndmeas.loc[:, 'measurement'] += meas_diff
        # filler measurement for alert level 1
        df_obv = pd.DataFrame({'meas_type': ['ROUTINE'], 'site_id': [site_id],
                               'weather': ['MAARAW'], 'observer_name':['TOPSSOFTWAREINFRA'],
                               'reliability': [1], 'data_source': ['SMS'],
                               'ts': [temp_gndmeas.ts[0]]})
        mo_id = int(db.df_write(data_table=sms.DataTable("marker_observations", 
            df_obv), resource='sensor_data', last_insert=True)[0][0])
        temp_gndmeas.loc[:, 'mo_id'] = mo_id
        df_data = temp_gndmeas.loc[:, ['mo_id', 'marker_id', 'measurement']]
        db.df_write(data_table = sms.DataTable("marker_data", df_data), resource='sensor_data')
        surf.generate_surficial_alert(site_id = site_id, ts = temp_gndmeas.ts[0])
    # measurement for ts given
    df_obv = pd.DataFrame({'meas_type': ['ROUTINE'], 'site_id': [site_id],
                           'weather': ['MAARAW'], 'observer_name':['TOPSSOFTWAREINFRA'],
                           'reliability': [1], 'data_source': ['SMS'],
                           'ts': [ts]})
    mo_id = int(db.df_write(data_table=sms.DataTable("marker_observations", 
        df_obv), resource='sensor_data', last_insert=True)[0][0])
    gndmeas.loc[:, 'mo_id'] = mo_id
    df_data = gndmeas.loc[:, ['mo_id', 'marker_id', 'measurement']]
    db.df_write(data_table = sms.DataTable("marker_data", df_data), resource='sensor_data')
    surf.generate_surficial_alert(site_id = site_id, ts = ts)
    
    # details for trigger tech info
    time_delta = np.round((ts - df.ts[0]).total_seconds()/3600, 2)
    if alert_level == 1:
        time_delta /= 2
    gndmeas.loc[:, 'displacement'] = meas_diff
    gndmeas.loc[:, 'time_delta'] = time_delta
    gndmeas.loc[:, 'alert_level'] = alert_level

    # writes to operational_triggers
    trigger_symbol = mem.get('df_trigger_symbols')
    trigger_sym_id = trigger_symbol.loc[(trigger_symbol.trigger_source == 'surficial') & (trigger_symbol.alert_level == alert_level), 'trigger_sym_id'].values[0]
    operational_trigger = pd.DataFrame({'site_id': [site_id], 'trigger_sym_id': [trigger_sym_id], 'ts': [ts], 'ts_updated': [ts]})
    qdb.alert_to_db(operational_trigger, 'operational_triggers')
    
    return gndmeas


def subsurface(site_id, ts, alert_level):
    """Insert values to node_alerts, tsm_alerts, and operational_triggers
    to (re)trigger subsurface alert.
    
    Args:
        site_id (int): ID of site to compute subsurface analysis for.
        ts (datetime): Timestamp of alert trigger.
        alert_level (int: {0, 2, 3}, default None): Subsurface alert level.
    """
    
    # get tsm_id
    query = "SELECT tsm_id FROM tsm_sensors "
    query += "where site_id = {} ".format(site_id)
    query += "and (date_deactivated is null or date_deactivated > '{}')".format(ts)
    tsm_id = db.df_read(query, resource='sensor_data').values.flatten()
    tsm_id = random.choice(tsm_id)
    
    # writes to node_alerts; defaults to node 1 and vel_alert
    ts_list = pd.date_range(end=ts, freq='30min', periods=4)
    node_alerts = pd.DataFrame({'ts': ts_list, 'node_id': [1]*len(ts_list),
                                'tsm_id': [tsm_id]*len(ts_list),
                                'disp_alert': [0]*len(ts_list),
                                'vel_alert': [alert_level]*len(ts_list)})
    db.df_write(data_table = sms.DataTable("node_alerts", node_alerts), resource='sensor_data')
    
    # writes to tsm_alerts
    tsm_alerts = pd.DataFrame({'ts': [ts], 'tsm_id': [tsm_id],
                               'alert_level': [alert_level],
                               'ts_updated': [ts]})
    db.df_write(data_table = sms.DataTable("tsm_alerts", tsm_alerts), resource='sensor_data')

    # writes to operational_triggers
    trigger_symbol = mem.get('df_trigger_symbols')
    trigger_sym_id = trigger_symbol.loc[(trigger_symbol.trigger_source == 'subsurface') & (trigger_symbol.alert_level == alert_level), 'trigger_sym_id'].values[0]
    operational_trigger = pd.DataFrame({'site_id': [site_id], 'trigger_sym_id': [trigger_sym_id], 'ts': [ts], 'ts_updated': [ts]})
    qdb.alert_to_db(operational_trigger, 'operational_triggers')

    # details for trigger tech info
    tsm_alerts.loc[:, 'node_id'] = 1
    tsm_alerts.loc[:, 'disp_alert'] = 0
    tsm_alerts.loc[:, 'vel_alert'] = alert_level
    
    return tsm_alerts


def earthquake(site_id, ts):
    """Insert values to earthquake_events, earthquake_alerts, and 
    operational_triggers to (re)trigger subsurface alert.
    
    Args:
        site_id (int): ID of site to compute earthquake analysis for.
        ts (datetime): Timestamp of alert trigger.
    """

    # writes to earthquake_events; defaults epicenter to site coordinates, depth to 0, and magnitude to 4.3
    sites = eq.get_sites()
    earthquake_events = sites.loc[sites.site_id == site_id, ['latitude', 'longitude', 'province']]
    earthquake_events.loc[:, 'ts'] = ts
    earthquake_events.loc[:, 'magnitude'] = 4.3
    earthquake_events.loc[:, 'depth'] = 0
    earthquake_events.loc[:, 'critical_distance'] = np.round(eq.get_crit_dist(4.3), decimals=2)
    earthquake_events.loc[:, 'issuer'] = 'TOPSSOFTWAREINFRA'
    earthquake_events.loc[:, 'processed'] = 1
    eq_id = int(db.df_write(data_table = sms.DataTable("earthquake_events", earthquake_events), resource='sensor_data', last_insert=True)[0][0])
    
    # writes to earthquake_alerts
    earthquake_alerts = pd.DataFrame({'eq_id': [eq_id], 'site_id': [site_id], 'distance': [0]})
    db.df_write(data_table = sms.DataTable("earthquake_alerts", earthquake_alerts), resource='sensor_data')
    
    # writes to operational_triggers
    trigger_symbol = mem.get('df_trigger_symbols')
    trigger_sym_id = trigger_symbol.loc[(trigger_symbol.trigger_source == 'earthquake') & (trigger_symbol.alert_level == 1), 'trigger_sym_id'].values[0]
    operational_trigger = pd.DataFrame({'site_id': [site_id], 'trigger_sym_id': [trigger_sym_id], 'ts': [ts], 'ts_updated': [ts]})
    qdb.alert_to_db(operational_trigger, 'operational_triggers')
    
    # details for trigger tech info
    earthquake_events.loc[:, 'distance'] = 0

    return earthquake_events