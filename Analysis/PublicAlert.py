import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time, date
from sqlalchemy import create_engine
import sys

import rtwindow as rtw
import querySenslopeDb as q
import alertgen as a
import AllRainfall as rain

def RoundTime(date_time):
    # rounds time to 4/8/12 AM/PM
    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4
    if quotient == 5:
        date_time = datetime.combine(date_time.date() + timedelta(1), time(0,0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0,0))
            
    return date_time

def getmode(li):
    li.sort()
    numbers = {}
    for x in li:
        num = li.count(x)
        numbers[x] = num
    highest = max(numbers.values())
    n = []
    for m in numbers.keys():
        if numbers[m] == highest:
            n.append(m)
    return n

def alert_toDB(df, table_name, window, source):
    # writes df to senslopedb.table_name; mode: append on change else upates 'updateTS'
    
    query = "SELECT * FROM senslopedb.%s WHERE site = '%s' AND source = '%s' AND updateTS <= '%s' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0], source, window.end)
    
    try:
        df2 = q.GetDBDataFrame(query)
    except:
        df2 = pd.DataFrame()        

    try:
        same_alert = df2['alert'].values[0] == df['alert'].values[0]
    except:
        same_alert = False

    query = "SELECT EXISTS(SELECT * FROM %s" %table_name
    query += " WHERE timestamp = '%s' AND site = '%s'" %(pd.to_datetime(df['updateTS'].values[0]), df['site'].values[0])
    query += " AND source = '%s')" %source

    if q.GetDBDataFrame(query).values[0][0] == 1:
        inDB = True
    else:
        inDB = False

    if (len(df2) == 0 or not same_alert) and not inDB:
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    elif same_alert and df2['updateTS'].values[0] < df['updateTS'].values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.%s SET updateTS='%s' WHERE site = '%s' and source = '%s' and alert = '%s' and timestamp = '%s'" %(table_name, window.end, df2.site.values[0], source, df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
        cur.execute(query)
        db.commit()
        db.close()

def SensorAlertLst(df, lst):
    sensor_alert = {}
    sensor_alert['sensor'] = df['site'].values[0]
    sensor_alert['alert'] = df['alert'].values[0]
    if sensor_alert not in lst:
        lst += [sensor_alert]

def SensorTrigger(df):
    sensor_tech = []
    for i in set(df['site'].values):
        col_df = df[df.site == i]
        if len(col_df) == 1:
            sensor_tech += ['%s (node %s)' %(i.upper(), col_df['id'].values[0])]
        else:
            sensor_tech += ['%s (nodes %s)' %(i.upper(), ','.join(sorted(col_df['id'].values)))]
    return ','.join(sensor_tech)

def alertgen(df, end):
    name = df['name'].values[0]
    query = "SELECT max(timestamp) FROM %s" %name
    ts = pd.to_datetime(q.GetDBDataFrame(query).values[0][0])
    if ts > end - timedelta(hours=12):
        if ts > end:
            ts = end
        try:
            a.main(name, end=ts, end_mon=True)
        except:
            pass

def SitePublicAlert(PublicAlert, window):
    site = PublicAlert['site'].values[0]
    print site
    
    # latest alert per source (rain,sensor,ground,internal,public,eq,on demand)*
    query = "(SELECT * FROM ( SELECT * FROM senslopedb.site_level_alert WHERE"
    query += " (updateTS <= '%s' OR (updateTS >= '%s' AND timestamp <= '%s'))" %(window.end, window.end, window.end)
    query += " AND ( site = '%s' " %site
    if site == 'bto':
        query += "or site = 'bat' "
    elif site == 'mng':
        query += "or site = 'man' "
    elif site == 'png':
        query += "or site = 'pan' "
    elif site == 'jor':
        query += "or site = 'pob' "
    elif site == 'tga':
        query += "or site = 'tag' "
    query += ") ORDER BY timestamp DESC) AS sub GROUP BY source)"

    # dataframe of *
    site_alert = q.GetDBDataFrame(query)
    
    # dataframe of all alerts
    validity_site_alert = site_alert.sort_values('updateTS', ascending = False)
    # dataframe of all alerts for the past 4hrs
    site_alert = site_alert.loc[site_alert.updateTS >= RoundTime(window.end) - timedelta(hours=4)]

    # public alert
    public_PrevAlert = validity_site_alert.loc[validity_site_alert.source == 'public']['alert'].values[0]
    
    # timestamp of start of monitoring
    # alert is still in effect or continuing operational trigger
    if 'A0' not in validity_site_alert['alert'].values:
        query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'public' AND alert != 'A0' ORDER BY timestamp DESC LIMIT 3" %site
        prev_PAlert = q.GetDBDataFrame(query)
        print 'Public Alert-', prev_PAlert['alert'].values[0]
        # one prev alert
        if len(prev_PAlert) == 1:
            start_monitor = pd.to_datetime(prev_PAlert.timestamp.values[0])
        # two prev alert
        elif len(prev_PAlert) == 2:
            # one event with two prev alert
            if pd.to_datetime(prev_PAlert['timestamp'].values[0]) - pd.to_datetime(prev_PAlert['updateTS'].values[1]) <= timedelta(hours=0.5):
                start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[1])
            else:
                start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[0])
        # three prev alert
        else:
            if pd.to_datetime(prev_PAlert['timestamp'].values[0]) - pd.to_datetime(prev_PAlert['updateTS'].values[1]) <= timedelta(hours=0.5):
                # one event with three prev alert
                if pd.to_datetime(prev_PAlert['timestamp'].values[1]) - pd.to_datetime(prev_PAlert['updateTS'].values[2]) <= timedelta(hours=0.5):
                    start_monitor = pd.to_datetime(prev_PAlert.timestamp.values[2])
                # one event with two prev alert
                else:
                    start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[1])
            else:
                start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[0])
    # occurrence of operational trigger
    elif 'r1' in site_alert['alert'].values or 'e1' in site_alert['alert'].values or 'd1' in site_alert['alert'].values \
            or 'l2' in site_alert['alert'].values or 'l3' in site_alert['alert'].values \
            or 'L2' in site_alert['alert'].values  or 'L3' in site_alert['alert'].values:
        start_monitor = window.end - timedelta(hours=4)
        if 'l3' in site_alert['alert'].values or 'L3' in site_alert['alert'].values:
            print 'Public Alert- A3'
        if 'l2' in site_alert['alert'].values or 'L2' in site_alert['alert'].values:
            print 'Public Alert- A2'
        if 'r1' in site_alert['alert'].values or 'e1' in site_alert['alert'].values or 'd1' in site_alert['alert'].values:
            print 'Public Alert- A1'

    try:
        new_ground_alert = validity_site_alert.loc[validity_site_alert.source == 'ground'].sort_values('timestamp', ascending=False)
        if validity_site_alert.loc[validity_site_alert.source == 'public']['alert'].values[0] == 'A0' and new_ground_alert['alert'].values[0] in ['l2', 'l3']:
            alertTS = pd.to_datetime(new_ground_alert['timestamp'].values[0])
            
            alertTS_year=alertTS.year
            alertTS_month=alertTS.month
            alertTS_day=alertTS.day
            alertTS_hour=alertTS.hour
            alertTS_minute=alertTS.minute
            if alertTS_minute<30:alertTS_minute=0
            else:alertTS_minute=30
            alertTS=datetime.combine(date(alertTS_year,alertTS_month,alertTS_day),time(alertTS_hour,alertTS_minute,0))

            start_monitor = alertTS
    except:
        pass

    # positive alerts within the non-A0 public alert
    try:

        # positive alerts from start of monitoring****
        query = "(SELECT * FROM senslopedb.site_level_alert WHERE ( site = '%s' " %site
        if site == 'bto':
            query += "or site = 'bat' "
        elif site == 'mng':
            query += "or site = 'man' "
        elif site == 'png':
            query += "or site = 'pan' "
        elif site == 'jor':
            query += "or site = 'pob' "
        elif site == 'tga':
            query += "or site = 'tag' "
        query += ") AND source IN ('sensor', 'ground', 'rain', 'eq', 'on demand') \
            AND alert not in ('ND', 'nd')\
            AND updateTS >= '%s' AND updateTS <= '%s' ORDER BY timestamp DESC)" %(start_monitor, window.end)
        
        # dataframe of *
        PAlert = q.GetDBDataFrame(query)

        SG_alert = PAlert.loc[(PAlert.source == 'sensor')|(PAlert.source == 'ground')]
        RED_alert = PAlert.loc[(PAlert.source == 'rain')|(PAlert.source == 'eq')|(PAlert.source == 'on demand')]
        other_alerts = ''
        if 'r1' in RED_alert['alert'].values:
            other_alerts += 'R'
        if 'e1' in RED_alert['alert'].values:
            other_alerts += 'E'
        if 'd1' in RED_alert['alert'].values:
            other_alerts += 'D'
        #last L2/L3 retriggger
        retriggerTS = []
        for i in ['l2', 'l3', 'L2', 'L3']:
            try:
                retriggerTS += [{'retrigger': i, 'timestamp': str(pd.to_datetime(max(SG_alert.loc[SG_alert.alert == i]['updateTS'].values)))}]
            except:
                continue        
        #last RED retriggger
        for i in ['r1', 'e1', 'd1']:
            try:
                retriggerTS += [{'retrigger': i, 'timestamp': str(pd.to_datetime(max(RED_alert.loc[RED_alert.alert == i]['updateTS'].values)))}]
            except:
                continue
        
    except:
        retriggerTS = []
        print 'Public Alert- A0'

    #technical info for bulletin release
    tech_info = {}
    retriggers = pd.DataFrame(retriggerTS)
    #rainfall technical info
    try:
        rain_techTS = retriggers[retriggers.retrigger == 'r1']['timestamp'].values[0]
        query = "SELECT * FROM senslopedb.rain_alerts where ts = '%s' and site_id = '%s'" %(rain_techTS, site)
        rain_tech_df = q.GetDBDataFrame(query)
        if 'r1a' in rain_tech_df['rain_alert'].values and 'r1b' in rain_tech_df['rain_alert'].values:
            r1a = rain_tech_df[rain_tech_df.rain_alert == 'r1a']
            r1b = rain_tech_df[rain_tech_df.rain_alert == 'r1b']
            rain_tech = '1-day and 3-day cumulative rainfall (%s mm and %s mm) exceeded threshold (%s mm and %s mm)' %(r1a['cumulative'].values[0], r1b['cumulative'].values[0], r1a['threshold'].values[0], r1b['threshold'].values[0])
        elif 'r1a' in rain_tech_df['rain_alert'].values:
            r1a = rain_tech_df[rain_tech_df.rain_alert == 'r1a']
            rain_tech = '1-day cumulative rainfall (%s mm) exceeded threshold (%s mm)' %(r1a['cumulative'].values[0], r1a['threshold'].values[0])
        else:
            r1b = rain_tech_df[rain_tech_df.rain_alert == 'r1b']
            rain_tech = '3-day cumulative rainfall (%s mm) exceeded threshold (%s mm)' %(r1b['cumulative'].values[0], r1b['threshold'].values[0])
        tech_info['rain_tech'] = rain_tech
    except:
        pass
    
    #surficial ground technical info
    try:
        ground_techTS = retriggers[(retriggers.retrigger == 'l2')|(retriggers.retrigger == 'l3')]['timestamp'].values[0]
        query = "SELECT * FROM marker_alerts where ts = '%s' and site_code = '%s' and alert != 'l0'" %(ground_techTS, site)
        ground_tech_df = q.GetDBDataFrame(query)
        ground_tech = []
        for i in set(ground_tech_df['marker_name'].values):
            disp = ground_tech_df[ground_tech_df.marker_name == i]['displacement'].values[0]
            time_delta = ground_tech_df[ground_tech_df.marker_name == i]['time_delta'].values[0]
            info = 'Crack %s: %s cm difference in %s hours' %(i, disp, time_delta)
            ground_tech += [info]
        ground_tech = ','.join(ground_tech)
        tech_info['ground_tech'] = ground_tech
    except:
        pass

    #earthquake technical info
    try:
        eq_techTS = retriggers[retriggers.retrigger == 'e1']['timestamp'].values[0]
        query = "SELECT ea.site_id, ea.distance, eq.mag, eq.lat, eq.longi, eq.critdist, eq.province \
            FROM earthquake_alerts as ea left join earthquake as eq on ea.eq_id = eq.e_id \
            where site_id = '%s' and timestamp = '%s' order by eq_id desc limit 1" %(site, eq_techTS)
        eq_tech_df = q.GetDBDataFrame(query)
        eq_tech = {'magnitude': np.round(eq_tech_df['mag'].values[0], 1), 'latitude': np.round(eq_tech_df['lat'].values[0], 2), 'longitude': np.round(eq_tech_df['longi'].values[0], 2)}
        if eq_tech_df['province'].values[0].lower() != 'null':
            eq_tech['info'] = str(np.round(eq_tech_df['distance'].values[0], 2)) + ' km away from earthquake at ' + eq_tech_df['province'].values[0].lower() + ' (inside critical radius of ' + str(np.round(eq_tech_df['critdist'].values[0], 2)) + ' km)'
        else:
            eq_tech['tech_info'] = str(np.round(eq_tech_df['distance'].values[0], 2)) + ' km away from earthquake epicenter (inside critical radius of ' + str(np.round(eq_tech_df['critdist'].values[0], 2)) + ' km)'
        tech_info['eq_tech'] = eq_tech
    except:
        pass

    #subsurface technical info
    try:
        sensor_techTS = retriggers[(retriggers.retrigger == 'L2')|(retriggers.retrigger == 'L3')]['timestamp'].values[0]
        query = "SELECT * FROM senslopedb.node_level_alert where timestamp >= '%s' and timestamp <= '%s' and site like '%s' order by timestamp desc" %(RoundTime(pd.to_datetime(sensor_techTS))-timedelta(hours=4), sensor_techTS, site+'%')
        sensor_tech_df = q.GetDBDataFrame(query)
        sensor_tech_df['tot_alert'] = sensor_tech_df['disp_alert'] + sensor_tech_df['vel_alert']
        sensor_tech_df = sensor_tech_df.sort_values(['col_alert', 'tot_alert', 'timestamp'], ascending=False)
        sensor_tech_df = sensor_tech_df.drop_duplicates(['site', 'id'])
        sensor_tech_df['id'] = sensor_tech_df['id'].apply(lambda x: str(x))
        both_trigger = sensor_tech_df[(sensor_tech_df.disp_alert == 1)&(sensor_tech_df.vel_alert == 1)]
        disp_trigger = sensor_tech_df[(sensor_tech_df.disp_alert == 1)&(sensor_tech_df.vel_alert == 0)]
        vel_trigger = sensor_tech_df[(sensor_tech_df.disp_alert == 0)&(sensor_tech_df.vel_alert == 1)]
        sensor_tech = []
        if len(both_trigger) != 0:
            dispvel_tech = SensorTrigger(both_trigger)
            sensor_tech += ['%s exceeded displacement and velocity threshold' %(dispvel_tech)]
        if len(disp_trigger) != 0:
            disp_tech = SensorTrigger(disp_trigger)
            sensor_tech += ['%s exceeded displacement threshold' %(disp_tech)]
        if len(vel_trigger) != 0:
            vel_tech = SensorTrigger(vel_trigger)
            sensor_tech += ['%s exceeded velocity threshold' %(vel_tech)]
        sensor_tech = ';'.join(sensor_tech)
        tech_info['sensor_tech'] = sensor_tech
    except:
        pass

    # latest column alert
    sensor_site = site + '%'
    query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site LIKE '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(sensor_site, window.end - timedelta(hours=0.5))
    sensor_alertDF = q.GetDBDataFrame(query)
    if len(sensor_alertDF) != 0:
        colsensor_alertDF = sensor_alertDF.groupby('site')
        sensor_alert = []
        colsensor_alertDF.apply(SensorAlertLst, lst=sensor_alert)
    else:
        sensor_alert = []
    
    # latest rain alert within 4hrs
    extend_nd_rain = False
    try:
        rain_alert = site_alert.loc[(site_alert.source == 'rain')]['alert'].values[0]
        if public_PrevAlert != 'A0':
            query = "SELECT * FROM senslopedb.rain_alerts where site_id = '%s' and ts = '%s'" %(site, window.end)
            rain_alert_df = q.GetDBDataFrame(query)
            if len(rain_alert_df) == 0 and rain_alert != 'nd':
                extend_rain_alert = True
            else:
                extend_rain_alert = False
        else:
            extend_rain_alert = False
    except:
        extend_rain_alert = False
        rain_alert = 'nd'
    
    #surficial data presence
    ground_alert = validity_site_alert.loc[(validity_site_alert.source == 'ground')&(validity_site_alert.updateTS >= RoundTime(window.end) - timedelta(hours=4))]
    if len(ground_alert) != 0:
        ground_alert = 'g'
    else:
        ground_alert = 'g0'

    try:
        SG_alert = SG_alert
    except:
        SG_alert = site_alert

    # LLMC ground alert for the past 4hrs and sensor alert
    surficial_alerts = SG_alert.loc[(SG_alert.updateTS >= RoundTime(window.end) - timedelta(hours=4))&(SG_alert.source == 'ground')]
    subsurface_alerts = SG_alert.loc[(SG_alert.updateTS >= window.end - timedelta(hours=0.5))&(SG_alert.source == 'sensor')]

    #Public Alert A3
    if 'L3' in SG_alert['alert'].values or 'l3' in SG_alert['alert'].values or 'A3' in validity_site_alert['alert'].values:
        validity_RED = RED_alert.loc[(RED_alert.alert == 'r1')|(RED_alert.alert == 'e1')|(RED_alert.alert == 'd1')]['updateTS'].values
        validity_L = SG_alert.loc[(SG_alert.alert == 'L3')|(SG_alert.alert == 'l3')|(SG_alert.alert == 'L2')|(SG_alert.alert == 'l2')]['updateTS'].values
        validity_A = site_alert.loc[(site_alert.alert == 'A3')]['timestamp'].values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_L) + list(validity_A) + list(validity_RED))))) + timedelta(2)
        
        # A3 is still valid
        if validity > window.end + timedelta(hours=0.5):
            public_alert = 'A3'
            # both ground and sensor triggered
            if ('L3' in SG_alert['alert'].values or 'L2' in SG_alert['alert'].values) and ('l3' in SG_alert['alert'].values or 'l2' in SG_alert['alert'].values):
                internal_alert = 'A3-SG' + other_alerts
            # sensor triggered
            elif 'L3' in SG_alert['alert'].values:
                internal_alert = 'A3-S' + other_alerts
            # ground triggered
            elif 'l3' in SG_alert['alert'].values:
                internal_alert = 'A3-G' + other_alerts
            
        # end of A3 validity
        else:          
            # both ground and sensor triggered
            if ('L3' in SG_alert['alert'].values or 'L2' in SG_alert['alert'].values) and ('l3' in SG_alert['alert'].values or 'l2' in SG_alert['alert'].values):
                # with data
                if len(subsurface_alerts) != 0 and len(surficial_alerts) != 0:
                    #if rainfall above 75%
                    if extend_rain_alert:
                        public_alert = 'A3'
                        internal_alert = 'A3-SG' + other_alerts
                        validity = RoundTime(window.end)
                        if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        # if nd rainfall alert                
                        if rain_alert == 'nd':
                            query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'rain' ORDER BY TIMESTAMP DESC LIMIT 2" %site
                            rain_alertDF = q.GetDBDataFrame(query)
                            # if rainfall alert is nd from r1
                            if rain_alertDF['alert'].values[1] == 'r1':
                                # within 1-day cap of 4H extension for nd
                                if RoundTime(window.end) - validity < timedelta(1):
                                    public_alert = 'A3'
                                    internal_alert = 'A3-SG' + other_alerts   
                                    validity = RoundTime(window.end)
                                    extend_nd_rain = True
                                # end of 1-day cap of 4H extension for nd
                                else:
                                    public_alert = 'A0'
                                    internal_alert = 'A0'
                                    validity = '-'
                            else:
                                public_alert = 'A0'
                                internal_alert = 'A0'
                                validity = '-'
                        else:
                            public_alert = 'A0'
                            internal_alert = 'A0'
                            validity = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if (RoundTime(window.end) - validity < timedelta(3)) or  extend_rain_alert:
                        public_alert = 'A3'
                        internal_alert = 'A3-SG' + other_alerts
                        validity = RoundTime(window.end)
                        if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'     
                    else:
                        public_alert = 'A0'
                        internal_alert = 'ND' 
                        validity = '-'                        

            # sensor triggered
            elif 'L3' in SG_alert['alert'].values:
                # with data
                if len(subsurface_alerts) != 0:
                    #if rainfall above 75%
                    if extend_rain_alert:
                        public_alert = 'A3'
                        internal_alert = 'A3-S' + other_alerts
                        validity = RoundTime(window.end)
                        if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        # if nd rainfall alert                
                        if rain_alert == 'nd':
                            query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'rain' ORDER BY TIMESTAMP DESC LIMIT 2" %site
                            rain_alertDF = q.GetDBDataFrame(query)
                            # if rainfall alert is nd from r1
                            if rain_alertDF['alert'].values[1] == 'r1':
                                # within 1-day cap of 4H extension for nd
                                if RoundTime(window.end) - validity < timedelta(1):
                                    public_alert = 'A3'
                                    internal_alert = 'A3-S' + other_alerts   
                                    validity = RoundTime(window.end)
                                    extend_nd_rain = True
                                # end of 1-day cap of 4H extension for nd
                                else:
                                    public_alert = 'A0'
                                    internal_alert = 'A0'
                                    validity = '-'
                            else:
                                public_alert = 'A0'
                                internal_alert = 'A0'
                                validity = '-'
                        else:
                            public_alert = 'A0'
                            internal_alert = 'A0'
                            validity = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if (RoundTime(window.end) - validity < timedelta(3)) or  extend_rain_alert:
                        public_alert = 'A3'
                        internal_alert = 'A3-S' + other_alerts
                        validity = RoundTime(window.end)
                        if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'                  
                    else:
                        public_alert = 'A0'
                        validity = '-'
                        internal_alert = 'ND'

            # ground triggered
            elif 'l3' in SG_alert['alert'].values:
                # with data
                if len(surficial_alerts) != 0:
                    #if rainfall above 75%
                    if extend_rain_alert:
                        public_alert = 'A3'
                        internal_alert = 'A3-G' + other_alerts
                        validity = RoundTime(window.end)
                        if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        # if nd rainfall alert                
                        if rain_alert == 'nd':
                            query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'rain' ORDER BY TIMESTAMP DESC LIMIT 2" %site
                            rain_alertDF = q.GetDBDataFrame(query)
                            # if rainfall alert is nd from r1
                            if rain_alertDF['alert'].values[1] == 'r1':
                                # within 1-day cap of 4H extension for nd
                                if RoundTime(window.end) - validity < timedelta(1):
                                    public_alert = 'A3'
                                    internal_alert = 'A3-G' + other_alerts   
                                    validity = RoundTime(window.end)
                                    extend_nd_rain = True
                                # end of 1-day cap of 4H extension for nd
                                else:
                                    public_alert = 'A0'
                                    internal_alert = 'A0'
                                    validity = '-'
                            else:
                                public_alert = 'A0'
                                internal_alert = 'A0'
                                validity = '-'
                        else:
                            public_alert = 'A0'
                            internal_alert = 'A0'
                            validity = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if (RoundTime(window.end) - validity < timedelta(3)) or extend_rain_alert:
                        public_alert = 'A3'
                        internal_alert = 'A3-G' + other_alerts
                        validity = RoundTime(window.end)
                        if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx' 
                    else:
                        public_alert = 'A0'
                        internal_alert = 'ND'
                        validity = '-'

        # replace S or G by s or g if L2 or l2 triggered only
        if 'S' in internal_alert:
            if 'L3' not in SG_alert['alert'].values:
                internal_alert = internal_alert.replace('S', 's')
        if 'G' in internal_alert:
            if 'l3' not in SG_alert['alert'].values:
                internal_alert = internal_alert.replace('G', 'g')

    #Public Alert A2
    elif 'L2' in SG_alert['alert'].values or 'l2' in SG_alert['alert'].values or 'A2' in validity_site_alert['alert'].values:
        validity_RED = RED_alert.loc[(RED_alert.alert == 'r1')|(RED_alert.alert == 'e1')|(RED_alert.alert == 'd1')]['updateTS'].values
        validity_L = SG_alert.loc[(SG_alert.alert == 'L2')|(SG_alert.alert == 'l2')]['updateTS'].values
        validity_A = site_alert.loc[(site_alert.alert == 'A2')]['timestamp'].values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_L) + list(validity_A) + list(validity_RED))))) + timedelta(1)
        
        # A2 is still valid
        if validity > window.end + timedelta(hours=0.5):
            public_alert = 'A2'

            # both ground and sensor triggered
            if 'L2' in SG_alert['alert'].values and 'l2' in SG_alert['alert'].values:
                if len(subsurface_alerts) != 0 and len(surficial_alerts) != 0:
                    internal_alert = 'A2-sg' + other_alerts
                else:
                    if len(subsurface_alerts) == 0 and len(surficial_alerts) == 0:
                        internal_alert = 'A2-s0g0' + other_alerts
                    else:
                        if len(subsurface_alerts) != 0:
                            internal_alert = 'A2-s'
                        else:
                            internal_alert = 'A2-s0'
                        if len(surficial_alerts) != 0:
                            internal_alert += 'g'
                        else:
                            internal_alert += 'g0'
                        internal_alert += other_alerts
            
            # sensor triggered
            elif 'L2' in SG_alert['alert'].values:
                if len(subsurface_alerts) != 0:
                    internal_alert = 'A2-s' + other_alerts
                else:
                    internal_alert = 'A2-s0' + other_alerts
                    
            # ground triggered
            elif 'l2' in SG_alert['alert'].values:
                if len(surficial_alerts) != 0:
                    internal_alert = 'A2-g' + other_alerts
                else:
                    internal_alert = 'A2-g0' + other_alerts

        # end of A2 validity if with data with no significant mov't
        else:
    
            # both ground and sensor triggered
            if 'L2' in SG_alert['alert'].values and 'l2' in SG_alert['alert'].values:
                # with data
                if len(subsurface_alerts) != 0 and len(surficial_alerts) != 0:
                    #if rainfall above 75%
                    if extend_rain_alert:
                        public_alert = 'A2'
                        internal_alert = 'A2-sg' + other_alerts
                        validity = RoundTime(window.end)
                        if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        public_alert = 'A0'
                        internal_alert = 'A0'
                        validity = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if (RoundTime(window.end) - validity < timedelta(3)) or extend_rain_alert:
                        public_alert = 'A2'
                        if len(subsurface_alerts) == 0 and len(surficial_alerts) == 0:
                            internal_alert = 'A2-s0g0' + other_alerts
                        else:
                            if len(subsurface_alerts) != 0:
                                internal_alert = 'A2-s'
                            else:
                                internal_alert = 'A2-s0'
                            if len(surficial_alerts) != 0:
                                internal_alert += 'g'
                            else:
                                internal_alert += 'g0'
                            internal_alert += other_alerts
                        validity = RoundTime(window.end)
                        if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        public_alert = 'A0'
                        internal_alert = 'ND'
                        validity = '-'

            # sensor triggered
            elif 'L2' in SG_alert['alert'].values:
                # with data
                if len(subsurface_alerts) != 0:
                    #if rainfall above 75%
                    if extend_rain_alert:
                        public_alert = 'A2'
                        internal_alert = 'A2-s' + other_alerts
                        validity = RoundTime(window.end)
                        if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        public_alert = 'A0'
                        internal_alert = 'A0'
                        validity = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if (RoundTime(window.end) - validity < timedelta(3)) or extend_rain_alert:
                        public_alert = 'A2'
                        internal_alert = 'A2-s0' + other_alerts
                        validity = RoundTime(window.end)
                        if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'

                    else:
                        public_alert = 'A0'
                        internal_alert = 'ND'
                        validity = '-'
                        
            # ground triggered
            elif 'l2' in SG_alert['alert'].values:
                # with data
                if len(surficial_alerts) != 0:
                    #if rainfall above 75%
                    if extend_rain_alert:
                        public_alert = 'A2'
                        internal_alert = 'A2-g' + other_alerts
                        validity = RoundTime(window.end)
                        if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                    else:
                        public_alert = 'A0'
                        internal_alert = 'A0'
                        validity = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if (RoundTime(window.end) - validity < timedelta(3)) or extend_rain_alert:
                        public_alert = 'A2'
                        internal_alert = 'A2-g0' + other_alerts
                        validity = RoundTime(window.end)
                        if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                            rain_alert = 'rx'
                            
                    else:
                        public_alert = 'A0'
                        internal_alert = 'ND'
                        validity = '-'

    #Public ALert A1
    elif 'r1' in site_alert['alert'].values or 'e1' in site_alert['alert'].values or 'd1' in site_alert['alert'].values or 'A1' in validity_site_alert['alert'].values:
        validity_RED = RED_alert.loc[(RED_alert.alert == 'r1')|(RED_alert.alert == 'e1')|(RED_alert.alert == 'd1')]['updateTS'].values
        validity_A = site_alert.loc[(site_alert.alert == 'A1')]['timestamp'].values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_RED) + list(validity_A))))) + timedelta(1)
        
        # A1 is still valid
        if validity > window.end + timedelta(hours=0.5):
            public_alert = 'A1'
            # identifies if with ground data
            if len(subsurface_alerts) != 0 or len(surficial_alerts) != 0:
                internal_alert = 'A1-' + other_alerts
            else:
                internal_alert = 'ND-' + other_alerts

        # end of A1 validity if with data with no significant mov't
        else:
            # with ground data
            if len(subsurface_alerts) != 0 or len(surficial_alerts) != 0:
                #if rainfall above 75%
                if extend_rain_alert:
                    public_alert = 'A1'
                    internal_alert = 'A1-' + other_alerts
                    validity = RoundTime(window.end)
                    if window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                        rain_alert = 'rx'
                else:
                    # if nd rainfall alert                
                    if rain_alert == 'nd':
                        query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'rain' ORDER BY TIMESTAMP DESC LIMIT 2" %site
                        rain_alertDF = q.GetDBDataFrame(query)
                        # if rainfall alert is nd from r1
                        if rain_alertDF['alert'].values[1] == 'r1':
                            # within 1-day cap of 4H extension for nd
                            if RoundTime(window.end) - validity < timedelta(1):
                                public_alert = 'A1'
                                internal_alert = 'A1-' + other_alerts   
                                validity = RoundTime(window.end)
                                extend_nd_rain = True
                            # end of 1-day cap of 4H extension for nd
                            else:
                                public_alert = 'A0'
                                internal_alert = 'A0'
                                validity = '-'
                        else:
                            public_alert = 'A0'
                            internal_alert = 'A0'
                            validity = '-'
                    else:
                        public_alert = 'A0'
                        internal_alert = 'A0'
                        validity = '-'
            
            # without ground data
            else:
                # within 3 days of 4hr-extension
                if (RoundTime(window.end) - validity < timedelta(3)) or extend_rain_alert:
                    public_alert = 'A1'        
                    internal_alert = 'ND-' + other_alerts
                    validity = RoundTime(window.end)
                    if extend_rain_alert and window.end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                        rain_alert = 'rx'        
                else:
                    public_alert = 'A0'
                    internal_alert = 'ND'
                    validity = '-'
    #Public Alert A0
    else:
        public_alert = 'A0'
        # latest alert per source (rain,sensor,ground,internal,public,eq,on demand)*
        query = "SELECT * FROM ("
        query += " SELECT * FROM site_level_alert"
        query += " WHERE site = '%s' AND" %site
        query += " ((source = 'public' and alert != 'A0')"
        query += " OR (source = 'ground' and timestamp >= '%s'))" %pd.to_datetime(window.end.date())
        query += " ORDER BY timestamp DESC"
        query += " ) AS sub GROUP BY source"

        # dataframe of *
        routine = q.GetDBDataFrame(query)
        
        if pd.to_datetime(routine[routine.source == 'public']['updateTS'].values[0]) <= pd.to_datetime(window.end.date()):
            surficial_alerts = surficial_alerts.append(routine[routine.source == 'ground'])
            if len(surficial_alerts) != 0:
                ground_alert = 'g'

        if len(subsurface_alerts) != 0 or len(surficial_alerts) != 0:
            internal_alert = 'A0'
        else:
            internal_alert = 'ND'
        validity = '-'
     
    alert_index = PublicAlert.loc[PublicAlert.site == site].index[0]
    if extend_nd_rain:
        internal_alert = internal_alert.replace('R', 'R0')
    
    palert_source = []
    try:
        if 's' in internal_alert.lower():
            palert_source += ['sensor']
        if 'g' in internal_alert.lower():
            palert_source += ['ground']
        if 'r' in internal_alert.lower():
            palert_source += ['rain']
        if 'e' in internal_alert.lower():
            palert_source += ['eq']
        if 'd' in internal_alert.lower().replace('nd', ''):
            palert_source += ['on demand']
    except:
        pass
    palert_source = ','.join(palert_source)
    
    nonND_alert = site_alert.loc[(site_alert.source != 'public')&(site_alert.source != 'internal')].dropna()
    if len(nonND_alert) != 0:
        ts = pd.to_datetime(str(nonND_alert.sort_values('updateTS', ascending = False)['updateTS'].values[0]))
        if ts > window.end:
            ts = window.end
        PublicAlert.loc[alert_index] = [ts, PublicAlert['site'].values[0], 'public', public_alert, window.end, palert_source, internal_alert, validity, sensor_alert, rain_alert, ground_alert, retriggerTS, tech_info]
    else:
        PublicAlert.loc[alert_index] = [window.end, PublicAlert['site'].values[0], 'public', public_alert, window.end, palert_source, internal_alert, validity, sensor_alert, rain_alert, ground_alert, retriggerTS, tech_info]
            
    SitePublicAlert = PublicAlert.loc[PublicAlert.site == site][['timestamp', 'site', 'source', 'alert', 'updateTS']]
    try:
        SitePublicAlert['timestamp'] = alertTS
    except:
        pass
    try:    
        alert_toDB(SitePublicAlert, 'site_level_alert', window, 'public')
    except:
        print 'duplicate'
    
    public_CurrAlert = SitePublicAlert['alert'].values[0]
        
    if public_CurrAlert != 'A0' and public_PrevAlert != public_CurrAlert:
        
        if public_CurrAlert == 'A3':
            smsAlertSource = SG_alert[(SG_alert['alert'] == 'l3')|(SG_alert['alert'] == 'L3')].sort_values('updateTS', ascending=False)
            smsAlertSource = smsAlertSource['source'].values[0]
        elif public_CurrAlert == 'A2':
            smsAlertSource = SG_alert[(SG_alert['alert'] == 'l2')|(SG_alert['alert'] == 'L2')].sort_values('updateTS', ascending=False)
            smsAlertSource = smsAlertSource['source'].values[0]
        
        try:
            GSMAlert = pd.DataFrame({'site': [site], 'alert': [public_CurrAlert], 'palert_source': [smsAlertSource]})
        except:
            GSMAlert = PublicAlert.loc[PublicAlert.site == site][['site', 'alert', 'palert_source']]        

        #node_level_alert
        if 's' in internal_alert or 'S' in internal_alert:
            query = "SELECT * FROM senslopedb.node_level_alert WHERE site LIKE '%s' AND timestamp >= '%s' ORDER BY timestamp DESC" %(sensor_site,start_monitor)
            allnode_alertDF = q.GetDBDataFrame(query)
            column_name = set(allnode_alertDF['site'].values)
            colnode_source = []
            for i in column_name:
                node_alertDF = allnode_alertDF.loc[allnode_alertDF.site == i]
                node_alert = list(set(node_alertDF['id'].values))
                node_alert = str(node_alert)[1:len(str(node_alert))-1].replace(' ', '')
                colnode_source += [str(i) + '-' + node_alert]
            colnode_source = 'sensor(' + ','.join(colnode_source) + ')'
            GSMAlert['palert_source'] = [GSMAlert['palert_source'].values[0].replace('sensor', colnode_source)]
        
        GSMAlert = GSMAlert[['site', 'alert', 'palert_source']]            
        with open('GSMAlert.txt', 'w') as w:
            w.write('As of ' + str(window.end)[:16] + '\n')
        GSMAlert.to_csv('GSMAlert.txt', header = False, index = None, sep = ':', mode = 'a')

        #write text file to db
        writeAlertToDb('GSMAlert.txt')
        
        with open('GSMAlert.txt', 'w') as w:
            w.write('')
            
    #sms alert for l0t
    groundTS = window.end - timedelta(hours=4)
    l0t_alert = validity_site_alert.loc[(validity_site_alert.alert == 'l0t') & (validity_site_alert.updateTS >= groundTS)]
    if len(l0t_alert) != 0:
        l0talert = site + ':l0t:ground'
        query = "SELECT * FROM senslopedb.smsalerts where alertmsg like '%s' and ts_set >= '%s' ORDER BY ts_set desc" %('%'+l0talert+'%', groundTS)
        df = q.GetDBDataFrame(query)
        if len(df) == 0:
            with open('l0t_alert.txt', 'w') as w:
                w.write('As of ' + str(datetime.now())[:16] + '\n')
                w.write(l0talert)
            writeAlertToDb('l0t_alert.txt')
            with open('l0t_alert.txt', 'w') as w:
                w.write('')

    if (public_CurrAlert == 'A0' and public_PrevAlert != public_CurrAlert) or (public_CurrAlert != 'A0' and window.end.time() in [time(7,30), time(19,30)]):
        query = "SELECT * FROM senslopedb.site_column_props where name LIKE '%s'" %sensor_site
        df = q.GetDBDataFrame(query)
        logger_df = df.groupby('name')
        logger_df.apply(alertgen, window.end)
        rain.main(site=site, end=window.end, monitoring_end=True)

    return PublicAlert

def writeAlertToDb(alertfile):
    f = open(alertfile,'r')
    alerttxt = f.read()
    f.close()

    sys.path.insert(0, '/home/dynaslope/Desktop/Senslope Server/')
    import senslopeServer as server

    server.writeAlertToDb(alerttxt)

def main(end=datetime.now()):
    start_time = datetime.now()
    
    with open('GSMAlert.txt', 'w') as w:
        w.write('')
        
    window,config = rtw.getwindow(end)
    
    props = q.GetRainProps('rain_props')
    PublicAlert = pd.DataFrame({'timestamp': [window.end]*len(props), 'site': props['name'].values, 'source': ['public']*len(props), 'alert': [np.nan]*len(props), 'updateTS': [window.end]*len(props), 'palert_source': [np.nan]*len(props), 'internal_alert': [np.nan]*len(props), 'validity': [np.nan]*len(props), 'sensor_alert': [[]]*len(props), 'rain_alert': [np.nan]*len(props), 'ground_alert': [np.nan]*len(props), 'retriggerTS': [[]]*len(props), 'tech_info': [{}]*len(props)})
    PublicAlert = PublicAlert[['timestamp', 'site', 'source', 'alert', 'updateTS', 'palert_source', 'internal_alert', 'validity', 'sensor_alert', 'rain_alert', 'ground_alert', 'retriggerTS', 'tech_info']]

    Site_Public_Alert = PublicAlert.groupby('site')
    PublicAlert = Site_Public_Alert.apply(SitePublicAlert, window=window)
    PublicAlert = PublicAlert[['timestamp', 'site', 'alert', 'internal_alert', 'palert_source', 'validity', 'sensor_alert', 'rain_alert', 'ground_alert', 'retriggerTS', 'tech_info']]
    PublicAlert = PublicAlert.rename(columns = {'palert_source': 'source'})
    PublicAlert = PublicAlert.sort_values(['alert', 'site'], ascending = [False, True])
    
    PublicAlert.to_csv('PublicAlert.txt', header=True, index=None, sep='\t', mode='w')
    
    PublicAlert['timestamp'] = PublicAlert['timestamp'].apply(lambda x: str(x))
    PublicAlert['validity'] = PublicAlert['validity'].apply(lambda x: str(x))
    public_json = PublicAlert.to_json(orient="records")
    
    invdf = pd.read_csv('InvalidAlert.txt', sep = ':')
    invdf['timestamp'] = invdf['timestamp'].apply(lambda x: str(x))
    inv_json = invdf.to_json(orient="records")

    df_json = dict({'alerts': public_json, 'invalids': inv_json})
    
    df_json = '[' + str(df_json).replace("\\\'", '').replace('\'', '').replace('alerts:', '"alerts":').replace('invalids:', '"invalids":') + ']'

    with open('PublicAlert.json', 'w') as w:
        w.write(df_json)

    print 'runtime =', datetime.now() - start_time

    return PublicAlert

################################################################################

if __name__ == "__main__":
    main()