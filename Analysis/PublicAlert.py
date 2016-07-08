import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from pandas.stats.api import ols
from sqlalchemy import create_engine
import sys

import cfgfileio as cfg
import rtwindow as rtw
import querySenslopeDb as q

def RoundTime(date_time):
    time_hour = int(date_time.strftime('%H'))
    time_min = int(date_time.strftime('%M'))
    if time_min > 0:
        time_hour += 1
    modulo = time_hour % 4
    
    if modulo == 0:
        date_time = datetime.combine(date_time.date(), time(time_hour,0,0))
    else:
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

def alert_toDB(df, table_name, window):
    
    query = "SELECT timestamp, site, source, alert FROM senslopedb.%s WHERE site = '%s' and source = 'public' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0])
    
    df2 = q.GetDBDataFrame(query)
    
    if len(df2) == 0 or df2.alert.values[0] != df.alert.values[0]:
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    elif df2.alert.values[0] == df.alert.values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.%s SET updateTS='%s' WHERE site = '%s' and source = 'public' and alert = '%s' and timestamp = '%s'" %(table_name, window.end, df2.site.values[0], df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
        cur.execute(query)
        db.commit()
        db.close()

def SitePublicAlert(PublicAlert, window):
    site = PublicAlert.site.values[0]
    print site
    
    query = "(SELECT * FROM ( SELECT * FROM senslopedb.site_level_alert WHERE ( site = '%s' " %site
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
    
    query += " UNION ALL "
    
    query += "(SELECT * FROM senslopedb.site_level_alert WHERE ( site = '%s' " %site
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
    query += ") AND source = 'sensor' AND alert in ('L2', 'L3') ORDER BY timestamp DESC LIMIT 4) "
    
    query += " UNION ALL "
    
    query += "(SELECT * FROM senslopedb.site_level_alert WHERE ( site = '%s' " %site
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
    query += ") AND source = 'rain' AND alert = 'r1' ORDER BY timestamp DESC LIMIT 1)"

    site_alert = q.GetDBDataFrame(query)
    
    validity_site_alert = site_alert
    site_alert = site_alert.loc[site_alert.updateTS >= window.end - timedelta(hours=3)]
    
    list_ground_alerts = ','.join(site_alert.alert.values)
    
    sensor_site = site + '%'
    query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site LIKE '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(sensor_site,window.end-timedelta(hours=3))
    sensor_alertDF = q.GetDBDataFrame(query)
    
    sensor_alert = str(sensor_alertDF[['site','alert']].set_index('site').T.to_dict('records'))
    sensor_alert = sensor_alert[2:len(sensor_alert)-2]
    
    try:
        rain_alert = site_alert.loc[(site_alert.source == 'rain') & (site_alert.updateTS >= window.end-timedelta(hours=3))].alert.values[0]
    except:
        rain_alert = 'nd'
    
    #Public Alert A3
    if 'L3' in site_alert.alert.values or 'l3' in site_alert.alert.values or 'A3' in validity_site_alert.alert.values:
        validity_L = validity_site_alert.loc[(validity_site_alert.alert == 'L3')|(validity_site_alert.alert == 'l3')].updateTS.values
        validity_A = site_alert.loc[(site_alert.alert == 'A3')].timestamp.values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_L) + list(validity_A))))) + timedelta(2)
        if validity >= window.end:
            public_alert = 'A3'
            internal_alert = 'A3'
            if 'L3' in site_alert.alert.values and 'l3' in site_alert.alert.values:
                alert_source = 'both ground and sensor'
            elif 'L3' in site_alert.alert.values:
                alert_source = 'sensor'
            elif 'l3' in site_alert.alert.values:
                alert_source = 'ground'
            else:
                alert_source = 'from last L3'
        else:
            public_alert = 'A0'
            alert_source = '-'
            validity = '-'
            if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                internal_alert = 'A0'
            else:
                internal_alert = 'ND'
    
    #Public Alert A2
    elif 'L2' in site_alert.alert.values or 'l2' in site_alert.alert.values or 'A2' in validity_site_alert.alert.values:
        validity_L = validity_site_alert.loc[(validity_site_alert.alert == 'L2')|(validity_site_alert.alert == 'l2')].updateTS.values
        validity_A = site_alert.loc[(site_alert.alert == 'A2')].timestamp.values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_L) + list(validity_A))))) + timedelta(1)
        if validity >= window.end:
            public_alert = 'A2'
            if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                internal_alert = 'A2'
            else:
                internal_alert = 'ND-L'
            if 'L2' in site_alert.alert.values and 'l2' in site_alert.alert.values:
                alert_source = 'both ground and sensor'
            elif 'L2' in site_alert.alert.values:
                alert_source = 'sensor'
            elif 'l2' in site_alert.alert.values:
                alert_source = 'ground'
            else:
                alert_source = 'from last L2'
        else:
            public_alert = 'A0'
            alert_source = '-'
            validity = '-'
            if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                internal_alert = 'A0'
            else:
                internal_alert = 'ND'

    #Public ALert A1
    elif 'r1' in site_alert.alert.values or 'e1' in site_alert.alert.values or 'd1' in site_alert.alert.values or 'A1' in validity_site_alert.alert.values:
        validity_RED = validity_site_alert.loc[(validity_site_alert.alert == 'r1')|(validity_site_alert.alert == 'e1')|(validity_site_alert.alert == 'd1')].updateTS.values
        validity_A = site_alert.loc[(site_alert.alert == 'A1')].timestamp.values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_RED) + list(validity_A))))) + timedelta(1)
        
        if validity >= window.end:
            public_alert = 'A1'
            if 'r1' in site_alert.alert.values:
                alert_source = 'r1'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-R'
                else:
                    internal_alert = 'ND-R'
            elif 'e1' in site_alert.alert.values:
                alert_source = 'e1'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-E'
                else:
                    internal_alert = 'ND-E'

            elif 'd1' in site_alert.alert.values:
                alert_source = 'd1'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-D'
                else:
                    internal_alert = 'ND-D'

            else:
                alert_source = ['from last A1']
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-R/E/D'
                else:
                    internal_alert = 'ND-R/E/D'
        else:
            public_alert = 'A0'
            alert_source = '-'
            validity = '-'
            if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                internal_alert = 'A0'
            else:
                internal_alert = 'ND'
    
    #Public Alert A0
    else:
        alert_source = '-'
        public_alert = 'A0'
        validity = '-'
        if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
            internal_alert = 'A0'
        else:
            internal_alert = 'ND'
    
    alert_index = PublicAlert.loc[PublicAlert.site == site].index[0]
    
    nonND_alert = site_alert.loc[(site_alert.source != 'public')&(site_alert.alert != 'nd')&(site_alert.alert != 'ND')].dropna()
    if len(nonND_alert) != 0:
        PublicAlert.loc[alert_index] = [pd.to_datetime(str(nonND_alert.sort('updateTS', ascending = False).updateTS.values[0])), PublicAlert.site.values[0], 'public', public_alert, window.end, alert_source, internal_alert, validity, sensor_alert, rain_alert]
    else:
        PublicAlert.loc[alert_index] = [pd.to_datetime('2016-01-01 00:00:00'), PublicAlert.site.values[0], 'public', 'A0', window.end, '-', 'ND', '-', 'ND', 'nd']
    
    SitePublicAlert = PublicAlert.loc[PublicAlert.site == site][['timestamp', 'site', 'source', 'alert', 'updateTS']]
    
    alert_toDB(SitePublicAlert, 'site_level_alert', window)

    return PublicAlert
    
def main():
    start = datetime.now()
    
    window,config = rtw.getwindow()
    PublicAlert = pd.DataFrame({'timestamp': [window.end]*len(q.GetRainProps()), 'site': q.GetRainProps().name.values, 'source': ['public']*len(q.GetRainProps()), 'alert': [np.nan]*len(q.GetRainProps()), 'updateTS': [window.end]*len(q.GetRainProps()), 'palert_source': [np.nan]*len(q.GetRainProps()), 'internal_alert': [np.nan]*len(q.GetRainProps()), 'validity': [np.nan]*len(q.GetRainProps()), 'sensor_alert': [np.nan]*len(q.GetRainProps()), 'rain_alert': [np.nan]*len(q.GetRainProps())})
    PublicAlert = PublicAlert[['timestamp', 'site', 'source', 'alert', 'updateTS', 'palert_source', 'internal_alert', 'validity', 'sensor_alert', 'rain_alert']]
    
    Site_Public_Alert = PublicAlert.groupby('site')
    
    PublicAlert = Site_Public_Alert.apply(SitePublicAlert, window=window)
    
    PublicAlert = PublicAlert[['timestamp', 'site', 'alert', 'palert_source', 'internal_alert', 'validity', 'sensor_alert', 'rain_alert']]
    PublicAlert = PublicAlert.rename(columns = {'palert_source': 'source'})
    print PublicAlert
    
    PublicAlert.to_csv('PublicAlert.txt', header=True, index=None, sep='\t', mode='w')
    
    dfjson = PublicAlert.to_json(orient="records", date_format="iso")
    with open('PublicAlert.json', 'w') as w:
        w.write(dfjson)
    
    print "run time =", datetime.now() - start
    
    return PublicAlert

################################################################################

if __name__ == "__main__":
    main()