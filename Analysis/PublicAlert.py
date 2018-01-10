import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time, date
import re
from sqlalchemy import create_engine
import sys

import querySenslopeDb as q
import alertgen as a
import AllRainfall as rain

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

def round_release_time(date_time):
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

def alert_toDB(df, table_name, end, source):
    # writes df to senslopedb.table_name; mode: append on change else upates 'updateTS'
    
    query = "SELECT * FROM senslopedb.%s WHERE site = '%s' AND source = '%s' AND updateTS <= '%s' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0], source, end)
    
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
        query = "UPDATE senslopedb.%s SET updateTS='%s' WHERE site = '%s' and source = '%s' and alert = '%s' and timestamp = '%s'" %(table_name, end, df2.site.values[0], source, df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
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
    if ts == None:
        return
    if ts > end - timedelta(hours=12):
        if ts > end:
            ts = end
        try:
            a.main(name, end=ts, end_mon=True)
        except:
            pass

def internal_alert_level(trigger):
    source = trigger['source'].values[0]
    
    if source == 'sensor':
        trigger['level'] = 1
    elif source == 'ground':
        trigger['level'] = 2
    elif source == 'moms':
        trigger['level'] = 3
    elif source == 'rain':
        trigger['level'] = 4
    elif source == 'eq':
        trigger['level'] = 5
    else:
        trigger['level'] = 6
        
    return trigger
    
def SitePublicAlert(PublicAlert, end):
    site = PublicAlert['site'].values[0]
    print site
    
    # latest alert per source (public,internal,sensor,ground,moms,rain,eq,on demand)*
    query = "(SELECT * FROM ( SELECT * FROM senslopedb.site_level_alert WHERE"
    query += " (updateTS <= '%s' OR (updateTS >= '%s' AND timestamp <= '%s'))" %(end, end, end)
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

    # dataframe of public alert and triggers within 24 hours
    site_alert = q.GetDBDataFrame(query)
    site_alert = site_alert[(site_alert.source == 'public') | ((site_alert.source != 'public') & (site_alert.updateTS >= end - timedelta(1)))]
    site_alert = site_alert[~site_alert.source.isin(['internal', 'noadjfilt', 'netvel'])]
    site_alert = site_alert[~site_alert.alert.isin(['nd', 'ND'])]

    #sms alert for l0t
    l0t_alert = site_alert.loc[site_alert.alert == 'l0t']
    if len(l0t_alert) != 0:
        groundTS = l0t_alert['timestamp'].values[0]
        l0talert = '%' + site + '%l0t%'
        query = "SELECT * FROM senslopedb.smsalerts where alertmsg like '%s' and ts_set >= '%s' ORDER BY ts_set desc" %(l0talert, groundTS)
        df = q.GetDBDataFrame(query)
        if len(df) == 0:
            with open('l0t_alert.txt', 'w') as w:
                w.write('As of ' + str(datetime.now())[:16] + '\n')
                w.write(l0talert)
            writeAlertToDb('l0t_alert.txt')
            with open('l0t_alert.txt', 'w') as w:
                w.write('')

    # public alert
    public_PrevAlert = site_alert.loc[site_alert.source == 'public']['alert'].values[0]

    # timestamp of start of monitoring
    # alert is still in effect or continuing operational trigger
    if 'A0' != public_PrevAlert:
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
    elif '1' in ','.join(site_alert['alert'].values) \
      or '2' in ','.join(site_alert['alert'].values) \
      or '3' in ','.join(site_alert['alert'].values):
        
        start_monitor = min(site_alert[(site_alert.source != 'public') & (~site_alert.alert.str.contains('0'))]['timestamp'].values)
        start_monitor = round_data_time(start_monitor)
        
        if '3' in ','.join(site_alert['alert'].values):
            print 'Public Alert- A3'
        elif '2' in ','.join(site_alert['alert'].values):
            print 'Public Alert- A2'
        else:
            print 'Public Alert- A1'
            
        init_triggerTS = start_monitor
        
    else:
        print 'Public Alert- A0'

    # positive alerts within the non-A0 public alert
    try:
    
        # positive alerts from start of monitoring****
        query =  "(SELECT * FROM senslopedb.site_level_alert "
        query += "WHERE ( site = '%s' " %site
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
        query += ") AND source NOT IN ('public', 'internal', 'noadjfilt', 'netvel') "
        query += "AND alert not in ('ND', 'nd') "
        query += "AND updateTS >= '%s' " %start_monitor
        query += "AND updateTS <= '%s' ORDER BY timestamp DESC)" %end
        
        # dataframe of *
        op_trigger = q.GetDBDataFrame(query)
        op_trigger = op_trigger.sort_values('timestamp', ascending=False)
    
        triggers = op_trigger[~op_trigger.alert.str.contains('0')]
        triggers = triggers.sort_values('alert', ascending=False)
        
        source_trigger = triggers.drop_duplicates('source')
        grp_trigger = source_trigger.groupby('source', as_index=False)
        source_trigger = grp_trigger.apply(internal_alert_level)
        source_trigger = source_trigger.sort_values('level')
        internal_alert = ''.join(source_trigger['alert'].values)
        internal_alert = internal_alert.replace('L3', 'S').replace('L2', 's')
        internal_alert = internal_alert.replace('l3', 'G').replace('l2', 'g')
        internal_alert = internal_alert.replace('M3', 'M').replace('M2', 'm')
        internal_alert = internal_alert.replace('r1', 'R')
        internal_alert = internal_alert.replace('i1', 'R')
        internal_alert = internal_alert.replace('e1', 'E')
        internal_alert = internal_alert.replace('d1', 'D')
        internal_alert = internal_alert.replace('RR', 'R')
    
        #last L2/L3 retriggger
        retriggerTS = []
        for i in set(triggers['alert'].values):
            retriggerTS += [{'retrigger': i, 'timestamp': str(pd.to_datetime(max(triggers.loc[triggers.alert == i]['updateTS'].values)))}]
        
    except:
        retriggerTS = []
        op_trigger = site_alert
        internal_alert = 'A0'
    retriggers = pd.DataFrame(retriggerTS)    

    
    #technical info for bulletin release
    tech_info = {}
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
            info = 'Marker %s: %s cm difference in %s hours' %(i, disp, time_delta)
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
        query =  "SELECT * FROM senslopedb.node_level_alert "
        query += "where timestamp >= '%s' " %(round_release_time(pd.to_datetime(sensor_techTS)) - timedelta(hours=4))
        query += "and timestamp <= '%s' and site like '%s' " %(sensor_techTS, site+'%')
        query += "order by timestamp desc"
        sensor_tech_df = q.GetDBDataFrame(query)
        sensor_tech_df['tot_alert'] = sensor_tech_df['disp_alert'] + sensor_tech_df['vel_alert']
        sensor_tech_df = sensor_tech_df.sort_values(['col_alert', 'tot_alert', 'timestamp'], ascending=False)
        sensor_tech_df = sensor_tech_df.drop_duplicates(['site', 'id'])
        sensor_tech_df['id'] = sensor_tech_df['id'].apply(lambda x: str(x))
        both_trigger = sensor_tech_df[(sensor_tech_df.disp_alert > 0)&(sensor_tech_df.vel_alert > 0)]
        disp_trigger = sensor_tech_df[(sensor_tech_df.disp_alert > 0)&(sensor_tech_df.vel_alert == 0)]
        vel_trigger = sensor_tech_df[(sensor_tech_df.disp_alert == 0)&(sensor_tech_df.vel_alert > 0)]
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
    query =  "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert "
    query += "WHERE site LIKE '%s' " %sensor_site
    query += "AND updateTS >= '%s' " %(end - timedelta(hours=0.5))
    query += "ORDER BY timestamp DESC) AS sub GROUP BY site"
    sensor_alert = q.GetDBDataFrame(query)
    sensor_alert = sensor_alert[['site', 'alert']]
    sensor_alert = sensor_alert.rename(columns = {'site': 'sensor'})
    if len(sensor_alert[sensor_alert.alert != 'ND']) == 0 and 's' in internal_alert.lower():
        internal_alert = internal_alert.replace('S', 'S0').replace('s', 's0')
    
    # latest rain alert within 4hrs
    try:
        rain_alert = op_trigger.loc[(op_trigger.source == 'rain')]['alert'].values[0]
    except:
        rain_alert = 'nd'
    
    # surficial data presence
    if internal_alert == 'A0':
        ground_ts = pd.to_datetime(end)
    else:
        ground_ts = round_release_time(end) - timedelta(hours=4)
    ground_alert = op_trigger.loc[(op_trigger.source == 'ground') & (op_trigger.updateTS >= ground_ts)]
    if len(ground_alert) != 0:
        ground_alert = 'g'
    else:
        ground_alert = 'g0'
    if 'g' in internal_alert.lower() and len(ground_alert) == 0:
        internal_alert = internal_alert.replace('G', 'G0').replace('g', 'g0')
        
    # qualitative surficial observation
    try:
        moms = op_trigger[op_trigger.source == 'moms']['alert'].values[0]
    except:
        moms = 'nd'
    
    #Public Alert A3
    if '3' in ','.join(op_trigger['alert'].values) or \
                '2' in ','.join(op_trigger['alert'].values) or \
                '1' in ','.join(op_trigger['alert'].values) or \
                public_PrevAlert != 'A0':
        public_alert = 'A' + max(re.findall(r'\d+', ','.join(op_trigger['alert'].values)))
        validity_trig = max(op_trigger.loc[~op_trigger.alert.str.contains('0')]['updateTS'].values)
        validity_A = site_alert.loc[(site_alert.source == 'public')]['timestamp'].values[0]
        validity = round_release_time(pd.to_datetime(max(validity_trig, validity_A)))
        if '3' in ','.join(op_trigger['alert'].values) or public_PrevAlert == 'A3':
            validity += timedelta(2)
        else:
            validity += timedelta(1)

        internal_alert = public_alert + '-' + internal_alert
        if public_alert == 'A1' and len(sensor_alert[sensor_alert.alert != 'ND']) == 0 and ground_alert == 'g0' and moms == 'nd':
            internal_alert = internal_alert.replace('A1', 'ND')
            
        if rain_alert == 'nd' and 'r' in internal_alert.lower():
            query =  "SELECT * FROM site_level_alert "
            query += "WHERE site = '%s' AND source = 'rain' " %site
            query += "ORDER BY timestamp DESC LIMIT 2"
            prev_rain_alert = q.GetDBDataFrame(query)['alert'].values[-1]
            if prev_rain_alert == 'nd':
                internal_alert = internal_alert.replace('R', 'R0')
        elif rain_alert == 'r0' and end > validity - timedelta(hours=0.5):
            query =  "SELECT * FROM senslopedb.rain_alerts "
            query += "where site_id = '%s' and ts = '%s'" %(site, end)
            rain_alert_df = q.GetDBDataFrame(query)
            if len(rain_alert_df) != 0 and 'r' in internal_alert.lower():
                internal_alert = internal_alert.replace('R', 'Rx')
            elif len(rain_alert_df) != 0:
                internal_alert += 'rx'
                internal_alert = internal_alert.replace('EDrx', 'rxED')
                internal_alert = internal_alert.replace('Drx', 'rxD')
                internal_alert = internal_alert.replace('Erx', 'rxE')

        # A3 is still valid
        if validity > end + timedelta(hours=0.5):
            pass
        elif ((validity + timedelta(3) > end + timedelta(hours=0.5)) and ('0' in internal_alert.lower() or 'nd' in internal_alert.lower())) or 'x' in internal_alert.lower():
            validity = round_release_time(end)
        elif end + timedelta(hours=0.5) >= validity + timedelta(3):
            public_alert = 'A0'
            internal_alert = 'ND'
            validity = ''        
        else:
            public_alert = 'A0'
            internal_alert = 'A0'
            validity = ''

    #Public Alert A0
    else:
        public_alert = 'A0'
        validity = ''
        if len(sensor_alert[sensor_alert.alert != 'ND']) == 0 and ground_alert == 'g0' and moms == 'nd':
            internal_alert = 'ND'
                
    source = []
    if 's' in internal_alert.lower():
        source += ['sensor']
    if 'g' in internal_alert.lower():
        source += ['ground']
    if 'm' in internal_alert.lower():
        source += ['moms']
    if 'r' in internal_alert.lower():
        source += ['rain']
    if 'e' in internal_alert.lower():
        source += ['eq']
    if 'd' in internal_alert.lower().replace('nd', ''):
        source += ['on demand']
    source = ','.join(source)

    if len(op_trigger) != 0:
        ts = pd.to_datetime(max(op_trigger['updateTS'].values))
        if ts > end:
            ts = end
        PublicAlert['timestamp'] = [ts]
    else:
        PublicAlert['timestamp'] = [end]
    
    PublicAlert['source'] = ['public']
    PublicAlert['alert'] = [public_alert]
    PublicAlert['updateTS'] = [end]
    PublicAlert['palert_source'] = [source]
    PublicAlert['internal_alert'] = [internal_alert]
    PublicAlert['validity'] = [validity]
    PublicAlert['sensor_alert'] = [sensor_alert]
    PublicAlert['rain_alert'] = [rain_alert]
    PublicAlert['ground_alert'] = [ground_alert]
    PublicAlert['retriggerTS'] = [retriggers]
    PublicAlert['tech_info'] = [tech_info]
            
    SitePublicAlert = PublicAlert.loc[PublicAlert.site == site][['timestamp', 'site', 'source', 'alert', 'updateTS']]

    try:
        SitePublicAlert['timestamp'] = init_triggerTS
    except:
        pass

    try:    
        alert_toDB(SitePublicAlert, 'site_level_alert', end, 'public')
    except:
        print 'duplicate'


    public_CurrAlert = SitePublicAlert['alert'].values[0]

    if public_CurrAlert != 'A0' or public_PrevAlert != public_CurrAlert:
        
        query =  "SELECT * FROM smsalerts "
        query += "where ts_set > '%s'" %(start_monitor - timedelta(1))
        prev_sms = q.GetDBDataFrame(query)
        init_triggers = site_alert[(site_alert.source != 'public') & (~site_alert.alert.str.contains('0'))]
        for i in range(len(init_triggers)):
            
            curr_source = init_triggers['source'].values[i]
            alertmsg = init_triggers['alert'].values[i][-1] + ':' + curr_source
            prev_source_sms = prev_sms[prev_sms.alertmsg.str.contains(alertmsg)&prev_sms.alertmsg.str.contains(site)]
            if len(prev_source_sms) == 0:
                GSMAlert = pd.DataFrame({'site': [site], 'alert': [public_CurrAlert], 'palert_source': [curr_source]})

                #node_level_alert
                if curr_source == 'sensor':
                    query =  "SELECT * FROM senslopedb.node_level_alert "
                    query += "WHERE site LIKE '%s' " %sensor_site
                    query += "AND timestamp >= '%s' ORDER BY timestamp DESC" %start_monitor
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
                    w.write('As of ' + str(end)[:16] + '\n')
                GSMAlert.to_csv('GSMAlert.txt', header = False, index = None, sep = ':', mode = 'a')
        
                #write text file to db
                writeAlertToDb('GSMAlert.txt')
            
                with open('GSMAlert.txt', 'w') as w:
                    w.write('')
            
    if (public_CurrAlert == 'A0' and public_PrevAlert != public_CurrAlert) or (public_CurrAlert != 'A0' and end.time() in [time(7,30), time(19,30)]):
        query = "SELECT * FROM senslopedb.site_column_props where name LIKE '%s'" %sensor_site
        df = q.GetDBDataFrame(query)
        logger_df = df.groupby('name')
        logger_df.apply(alertgen, end)
        rain.main(site=site, end=end, monitoring_end=True)

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
    print start_time
    
    with open('GSMAlert.txt', 'w') as w:
        w.write('')
        
    end = round_data_time(end)
    
    props = q.GetRainProps('rain_props')
    site_df = pd.DataFrame({'site': props['name'].values})

    Site_Public_Alert = site_df.groupby('site')
    PublicAlert = Site_Public_Alert.apply(SitePublicAlert, end=end)
    PublicAlert = PublicAlert[['timestamp', 'site', 'alert', 'internal_alert', 'palert_source', 'validity', 'sensor_alert', 'rain_alert', 'ground_alert', 'retriggerTS', 'tech_info']]
    PublicAlert = PublicAlert.rename(columns = {'palert_source': 'source'})
    PublicAlert = PublicAlert.sort_values(['alert', 'site'], ascending = [False, True])
    
    PublicAlert.to_csv('PublicAlert.txt', header=True, index=None, sep='\t', mode='w')
    
    PublicAlert['timestamp'] = PublicAlert['timestamp'].apply(lambda x: str(x))
    PublicAlert['validity'] = PublicAlert['validity'].apply(lambda x: str(x))

    invdf = pd.read_csv('InvalidAlert.txt', sep = ':')
    invdf['timestamp'] = invdf['timestamp'].apply(lambda x: str(x))
    
    df = pd.DataFrame({'invalids': [invdf], 'alerts': [PublicAlert]})
    json = df.to_json(orient="records")

    with open('PublicAlert.json', 'w') as w:
        w.write(json)

    print 'runtime =', datetime.now() - start_time

    return PublicAlert

################################################################################

if __name__ == "__main__":
    main()