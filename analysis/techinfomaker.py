from datetime import datetime, timedelta, time
import pandas as pd
import querydb as qdb

def release_time(date_time):
    """Rounds time to 4/8/12 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off. 04:00 to 07:30 is
        rounded off to 8:00, 08:00 to 11:30 to 12:00, etc.

    Returns:
        datetime: Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time

def query_tsm_alerts(site_id, start_ts, latest_trigger_ts):   
        query =  "SELECT ts, t_s.tsm_name, node_id, disp_alert, vel_alert FROM node_alerts "
        query += "JOIN tsm_sensors AS t_s"
        query += "  ON node_alerts.tsm_id = t_s.tsm_id "
        query += "JOIN "
        query += "  (SELECT site_code, site_id FROM sites WHERE site_id = '%s') AS sc " %(site_id)
        query += "  ON t_s.site_id = sc.site_id "
        query += "WHERE ts >= '%s' " %(start_ts)
        query += "AND ts <= '%s' " %(latest_trigger_ts) 
        query += "AND t_s.tsm_name LIKE CONCAT(sc.site_code, '%') "
        query += "ORDER BY ts DESC"  
        result = qdb.get_db_dataframe(query)
        
        return result

def query_rainfall_alerts(site_id, latest_trigger_ts):
        query = "SELECT ra.*, rp.* "
        query += "FROM rainfall_alerts AS ra "
        query += "JOIN rain_props AS rp "
        query += "ON ra.rain_id = rp.rain_id "
        query += "WHERE ra.site_id = '%s' AND ts = '%s'" %(site_id, latest_trigger_ts)
        result = qdb.get_db_dataframe(query)
        
        return result
    
def query_surficial_alerts(site_id, latest_trigger_ts):
        query = "SELECT * FROM marker_alerts as ma "
        query += "JOIN site_markers as sm "
        query += "ON ma.marker_id = sm.marker_id "
        query += "WHERE sm.site_id = '%s' and ts = '%s'" %(site_id, latest_trigger_ts)
        query += "AND alert_level > 0"
        result = qdb.get_db_dataframe(query)
        
        return result

def format_node_details(result):
    node_details = []
    for i in set(result['tsm_name'].values):
        col_df = result[result.tsm_name == i]
        
        if len(col_df) == 1:
            node_details += ['%s (node %s)' %(i.upper(), col_df['node_id'].values[0])]
        else:
            sorted_nodes = sorted(col_df['node_id'].values)
            node_details += ['%s (nodes %s)' %(i.upper(), ', '.join(str(v) \
                                               for v in sorted_nodes))]
    
    return ','.join(node_details)

def formulate_subsurface_tech_info(alert_detail):
    both_trigger = alert_detail[(alert_detail.disp_alert > 0) & (alert_detail.vel_alert > 0)]
    disp_trigger = alert_detail[(alert_detail.disp_alert > 0) & (alert_detail.vel_alert == 0)]
    vel_trigger = alert_detail[(alert_detail.disp_alert == 0) & (alert_detail.vel_alert > 0)]
    node_details = []
    
    if len(both_trigger) != 0:
        dispvel_tech = format_node_details(both_trigger)
        node_details += ['%s exceeded displacement and velocity threshold' %(dispvel_tech)]
    
    if len(disp_trigger) != 0:
        disp_tech = format_node_details(disp_trigger)
        node_details += ['%s exceeded displacement threshold' %(disp_tech)]
    
    if len(vel_trigger) != 0:
        vel_tech = format_node_details(vel_trigger)
        node_details += ['%s exceeded velocity threshold' %(vel_tech)]
        
    node_details = '; '.join(node_details)
    return node_details

def get_subsurface_tech_info(site_id, start_ts, latest_trigger_ts):
        alert_detail = query_tsm_alerts(site_id, start_ts, latest_trigger_ts)
        alert_detail = alert_detail.drop_duplicates(['tsm_name', 'node_id'])
        
        l2_triggers = alert_detail[(alert_detail.disp_alert == 2) | (alert_detail.vel_alert == 2)]
        l3_triggers = alert_detail[(alert_detail.disp_alert == 3) | (alert_detail.vel_alert == 3)]

        subsurface_tech_info = {}
        group_array = [l2_triggers, l3_triggers]
        for index, group in enumerate(group_array):
            if (len(group) != 0):
                tech_info = formulate_subsurface_tech_info(group)
                subsurface_tech_info["L" + str(index+2)] = tech_info
                
        return subsurface_tech_info

def get_rainfall_tech_info(site_id, latest_trigger_ts):
    alert_detail = query_rainfall_alerts(site_id, latest_trigger_ts)
    
    rain_gauge = alert_detail['gauge_name'][0]
    if alert_detail['data_source'][0] == "noah":
        rain_gauge = "NOAH " + str(rain_gauge)
    rain_gauge = rain_gauge.upper()
    
    one_day_data = alert_detail[(alert_detail.rain_alert == 'a')]
    three_day_data = alert_detail[(alert_detail.rain_alert == 'b')].reset_index(drop=True)
    
    days = []
    cumulatives = []
    thresholds = []
    
    if len(one_day_data) == 1:
        days += ["1-day"]
        cumulatives += ['{:.2f}'.format(one_day_data['cumulative'][0])]
        thresholds += ['{:.2f}'.format(one_day_data['threshold'][0])]
                
    if len(three_day_data) == 1:
        days += ["3-day"]
        cumulatives += ['{:.2f}'.format(three_day_data['cumulative'][0])]
        thresholds += ['{:.2f}'.format(three_day_data['threshold'][0])]
    
    day = ' and '.join(days)
    cumulative = ' and '.join(cumulatives)
    threshold = ' and '.join(thresholds)    
    
    rain_tech_info = "RAIN %s: %s cumulative rainfall " %(rain_gauge, day)
    rain_tech_info += "(%s mm) exceeded threshold (%s mm)" %(cumulative, threshold)
    return rain_tech_info  

def formulate_surficial_tech_info(alert_detail):
    tech_info = []
    for index, row in alert_detail.iterrows():
        name = row['marker_name']
        disp = row['displacement']
        time = '{:.2f}'.format(row['time_delta'])
        tech_info += ["Marker %s: %s cm difference in %s hours" %(name, disp, time)]
    
    surficial_tech_info = '; '.join(tech_info)
    
    return surficial_tech_info
        
def get_surficial_tech_info(site_id, latest_trigger_ts):
    alert_detail = query_surficial_alerts(site_id, latest_trigger_ts)
    
    l2_triggers = alert_detail[(alert_detail.alert_level == 2)]
    l3_triggers = alert_detail[(alert_detail.alert_level == 3)]
    
    surficial_tech_info = {}
    group_array = [l2_triggers, l3_triggers]
    for index, group in enumerate(group_array):
        if (len(group) != 0):
            tech_info = formulate_surficial_tech_info(group)
            surficial_tech_info["L" + str(index+2)] = tech_info
            
    return surficial_tech_info
        
def main(trigger_df):
    # print trigger_df
    trigger_group = trigger_df.groupby('trigger_source', as_index=False)
    site_id = trigger_df.iloc[0]['site_id']
    
    technical_info = {}
    
    for trigger_source, group in trigger_group:
        latest_trigger_ts = group.iloc[0]['ts_updated']
        start_ts = release_time(pd.to_datetime(latest_trigger_ts)) \
                    - timedelta(hours=4)
        
        if trigger_source == 'subsurface':
            technical_info['subsurface'] = get_subsurface_tech_info(site_id, start_ts, latest_trigger_ts)
        elif trigger_source == 'rainfall':
            technical_info['rainfall'] = get_rainfall_tech_info(site_id, latest_trigger_ts)
        elif trigger_source == 'surficial':
            technical_info['surficial'] = get_surficial_tech_info(site_id, latest_trigger_ts)
    
    return technical_info