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
    
    query = "SELECT * FROM senslopedb.%s WHERE site = '%s' and source = '%s' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0], source)
    
    df2 = q.GetDBDataFrame(query)
    
    if len(df2) == 0 or df2.alert.values[0] != df.alert.values[0]:
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    elif df2.alert.values[0] == df.alert.values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.%s SET updateTS='%s' WHERE site = '%s' and source = '%s' and alert = '%s' and timestamp = '%s'" %(table_name, window.end, df2.site.values[0], source, df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
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
    elif site == 'msl':
        query += "or site = 'mes' "
    elif site == 'msu':
        query += "or site = 'mes' "
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
    query += ") AND source = 'sensor' AND alert IN ('L2', 'L3') ORDER BY timestamp DESC LIMIT 2) "
    
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
    elif site == 'msl':
        query += "or site = 'mes' "
    elif site == 'msu':
        query += "or site = 'mes' "
    query += ") AND source = 'ground' AND alert IN ('l2', 'l3') ORDER BY timestamp DESC LIMIT 2)"

    site_alert = q.GetDBDataFrame(query)
    
    validity_site_alert = site_alert
    site_alert = site_alert.loc[site_alert.updateTS >= window.end - timedelta(hours=3)]
    
    list_ground_alerts = ','.join(site_alert.loc[(site_alert.source == 'sensor')|(site_alert.source == 'ground')].alert.values)
    
    public_PrevAlert = validity_site_alert.loc[validity_site_alert.source == 'public'].alert.values[0]
    
    internal_alertDF = site_alert.loc[(site_alert.source == 'internal')|(site_alert.alert == 'ND-L')|(site_alert.alert == 'ND-R')|(site_alert.alert == 'ND-E')|(site_alert.alert == 'ND-D')]
    
    Pub_PAlertTS = validity_site_alert.loc[(validity_site_alert.source == 'public') & (validity_site_alert.alert != 'A0')]

    if len(Pub_PAlertTS) != 0:
        SG_PAlert = validity_site_alert.loc[(validity_site_alert.updateTS >= Pub_PAlertTS.timestamp.values[0]) & ((validity_site_alert.source == 'sensor')|(validity_site_alert.source == 'ground'))]
    
    sensor_site = site + '%'
    if site == 'msl':
        sensor_site = 'messb%'
    if site == 'msu':
        sensor_site = 'mesta%'
    query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site LIKE '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(sensor_site,window.end-timedelta(hours=3))
    sensor_alertDF = q.GetDBDataFrame(query)
    
    sensor_alert = str(sensor_alertDF[['site','alert']].set_index('site').T.to_dict('records'))
    sensor_alert = sensor_alert[2:len(sensor_alert)-2]
    
    try:
        rain_alert = site_alert.loc[(site_alert.source == 'rain') & (site_alert.updateTS >= window.end-timedelta(hours=3))].alert.values[0]
    except:
        rain_alert = 'nd'
    
    #Public Alert A3
    if 'L3' in site_alert.alert.values or 'l3' in site_alert.alert.values or 'A3-SG' in validity_site_alert.alert.values or 'A3-S' in validity_site_alert.alert.values or 'A3-G' in validity_site_alert.alert.values:
        validity_RED = validity_site_alert.loc[(validity_site_alert.alert == 'r1')|(validity_site_alert.alert == 'e1')|(validity_site_alert.alert == 'd1')].updateTS.values
        validity_L = validity_site_alert.loc[(validity_site_alert.alert == 'L3')|(validity_site_alert.alert == 'l3')|(validity_site_alert.alert == 'L2')|(validity_site_alert.alert == 'l2')].updateTS.values
        validity_A = site_alert.loc[(site_alert.alert == 'A3-SG')|(site_alert.alert == 'A3-S')|(site_alert.alert == 'A3-G')].timestamp.values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_L) + list(validity_A) + list(validity_RED))))) + timedelta(2)
        # A3 is still valid
        if validity >= window.end - timedelta(hours=3.5):
            public_alert = 'A3'
            # evaluates which triggers A2
            if ('L3' in SG_PAlert.alert.values or 'L2' in SG_PAlert.alert.values) and ('l3' in SG_PAlert.alert.values or 'l2' in SG_PAlert.alert.values):
                alert_source = 'both ground and sensor'
                internal_alert = 'A3-SG'
            elif 'L3' in SG_PAlert.alert.values:
                alert_source = 'sensor'
                internal_alert = 'A3-S'
            else:
                alert_source = 'ground'
                internal_alert = 'A3-G'
        # end of A3 validity
        else:
            public_alert = 'A0'
            alert_source = '-'
            validity = '-'
            if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                internal_alert = 'A0'
            else:
                internal_alert = 'ND'
    
    #Public Alert A2
    elif 'L2' in site_alert.alert.values or 'l2' in site_alert.alert.values or 'A2-SG' in validity_site_alert.alert.values or 'A2-S' in validity_site_alert.alert.values or 'A2-G' in validity_site_alert.alert.values:
        validity_RED = validity_site_alert.loc[(validity_site_alert.alert == 'r1')|(validity_site_alert.alert == 'e1')|(validity_site_alert.alert == 'd1')].updateTS.values
        validity_L = validity_site_alert.loc[(validity_site_alert.alert == 'L2')|(validity_site_alert.alert == 'l2')].updateTS.values
        validity_A = site_alert.loc[(site_alert.alert == 'A2-SG')|(site_alert.alert == 'A2-S')|(site_alert.alert == 'A2-G')].timestamp.values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_L) + list(validity_A) + list(validity_RED))))) + timedelta(1)
        
        # A2 is still valid
        if validity > window.end + timedelta(hours=0.5):
            public_alert = 'A2'

            # evaluates which triggers A2
            if 'L2' in SG_PAlert.alert.values and 'l2' in SG_PAlert.alert.values:
                alert_source = 'both ground and sensor'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A2-SG'
                else:
                    internal_alert = 'ND-SG'
            elif 'L2' in SG_PAlert.alert.values:
                alert_source = 'sensor'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A2-S'
                else:
                    internal_alert = 'ND-S'
            else:
                alert_source = 'ground'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A2-G'
                else:
                    internal_alert = 'ND-G'

        # end of A2 validity if with data with no significant mov't
        else:
    
            # evaluates which triggers A2
            if 'L2' in SG_PAlert.alert.values and 'l2' in SG_PAlert.alert.values:
                alert_source = 'both ground and sensor'
            elif 'L2' in SG_PAlert.alert.values:
                alert_source = 'sensor'
            else:
                alert_source = 'ground'

            # both ground and sensor triggered
            if alert_source == 'both ground and sensor':
                # with data
                if 'L' in list_ground_alerts and 'l' in list_ground_alerts:
                    internal_alert = 'A0'
                    public_alert = 'A0'
                    alert_source = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if len(internal_alertDF) == 0 or pd.to_datetime(internal_alertDF.timestamp.values[0]) >= (window.end - timedelta(3)):
                        validity = validity + timedelta(hours=4)
                        internal_alert = 'ND-SG'
                        public_alert = 'A2-SG'
                        
                    else:
                        public_alert = 'A0'
                        alert_source = '-'
                        validity = '-'
                        internal_alert = 'ND'

            # sensor triggered
            elif alert_source == 'sensor':
                # with data
                if 'L' in list_ground_alerts:
                    internal_alert = 'A0'
                    public_alert = 'A0'
                    alert_source = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if len(internal_alertDF) == 0 or pd.to_datetime(internal_alertDF.timestamp.values[0]) >= (window.end - timedelta(3)):
                        validity = validity + timedelta(hours=4)
                        internal_alert = 'ND-S'
                        public_alert = 'A2-S'
                        
                    else:
                        public_alert = 'A0'
                        alert_source = '-'
                        validity = '-'
                        internal_alert = 'ND'

            # ground triggered
            else:
                # with data
                if 'l' in list_ground_alerts:
                    internal_alert = 'A0'
                    public_alert = 'A0'
                    alert_source = '-'
                # without data
                else:
                    # within 3 days of 4hr-extension
                    if len(internal_alertDF) == 0 or pd.to_datetime(internal_alertDF.timestamp.values[0]) >= (window.end - timedelta(3)):
                        validity = validity + timedelta(hours=4)
                        internal_alert = 'ND-G'
                        public_alert = 'A2-G'
                        
                    else:
                        public_alert = 'A0'
                        alert_source = '-'
                        validity = '-'
                        internal_alert = 'ND'

            
    #Public ALert A1
    elif 'r1' in site_alert.alert.values or 'e1' in site_alert.alert.values or 'd1' in site_alert.alert.values or 'A1' in validity_site_alert.alert.values:
        validity_RED = validity_site_alert.loc[(validity_site_alert.alert == 'r1')|(validity_site_alert.alert == 'e1')|(validity_site_alert.alert == 'd1')].updateTS.values
        validity_A = site_alert.loc[(site_alert.alert == 'A1')].timestamp.values
        validity = RoundTime(pd.to_datetime(str(max(list(validity_RED) + list(validity_A))))) + timedelta(1)
        
        # A1 is still valid
        if validity > window.end + timedelta(hours=0.5):
            public_alert = 'A1'
            # rain (re)trigger
            if 'r1' in site_alert.alert.values:
                alert_source = 'rain'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-R'
                else:
                    internal_alert = 'ND-R'
                    if validity == window.end:
                        validity = validity + timedelta(hours=4)
            # earthquake (re)trigger
            elif 'e1' in site_alert.alert.values:
                alert_source = 'eq'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-E'
                else:
                    internal_alert = 'ND-E'
                    if validity == window.end:
                        validity = validity + timedelta(hours=4)
            # on demand (re)trigger
            elif 'd1' in site_alert.alert.values:
                alert_source = 'on demand'
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    internal_alert = 'A1-D'
                else:
                    internal_alert = 'ND-D'
                    if validity == window.end:
                        validity = validity + timedelta(hours=4)
            # A1 is still valid
            else:
                if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                    if 'A1-R' in validity_site_alert.alert.values or 'ND-R' in validity_site_alert.alert.values:
                        internal_alert = 'A1-R'
                        alert_source = 'rain'
                    elif 'A1-E' in validity_site_alert.alert.values or 'ND-E' in validity_site_alert.alert.values:
                        internal_alert = 'A1-E'
                        alert_source = 'eq'
                    elif 'A1-D' in validity_site_alert.alert.values or 'ND-D' in validity_site_alert.alert.values:
                        internal_alert = 'A1-D'
                        alert_source = 'on demand'
                else:
                    if 'A1-R' in validity_site_alert.alert.values or 'ND-R' in validity_site_alert.alert.values:
                        internal_alert = 'ND-R'
                        alert_source = 'rain'
                    elif 'A1-E' in validity_site_alert.alert.values or 'ND-E' in validity_site_alert.alert.values:
                        internal_alert = 'ND-E'
                        alert_source = 'eq'
                    elif 'A1-D' in validity_site_alert.alert.values or 'ND-D' in validity_site_alert.alert.values:
                        internal_alert = 'ND-D'
                        alert_source = 'on demand'

        # end of A1 validity if with data with no significant mov't
        else:
            # with data
            if 'L' in list_ground_alerts or 'l' in list_ground_alerts:
                internal_alert = 'A0'
                public_alert = 'A0'
                alert_source = '-'
                validity = '-'
            
            # without data
            else:
                # within 3 days of 4hr-extension
                if len(internal_alertDF) == 0 or pd.to_datetime(internal_alertDF.timestamp.values[0]) >= (window.end - timedelta(3)):
                    validity = validity + timedelta(hours=4)
                    public_alert = 'A1'
                    
                    # evaluates which triggers A1
                    if 'A1-R' in validity_site_alert.alert.values or 'ND-R' in validity_site_alert.alert.values:
                        internal_alert = 'ND-R'
                        alert_source = 'rain'
                    elif 'A1-E' in validity_site_alert.alert.values or 'ND-E' in validity_site_alert.alert.values:
                        internal_alert = 'ND-E'
                        alert_source = 'eq'
                    elif 'A1-D' in validity_site_alert.alert.values or 'ND-D' in validity_site_alert.alert.values:
                        internal_alert = 'ND-D'
                        alert_source = 'on demand'

                else:
                    public_alert = 'A0'
                    alert_source = '-'
                    validity = '-'
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
        
    InternalAlert = PublicAlert.loc[PublicAlert.site == site][['timestamp', 'site', 'internal_alert', 'updateTS']]
    InternalAlert['source'] = 'internal'
    InternalAlert = InternalAlert.rename(columns = {'internal_alert': 'alert'})
    InternalAlert = InternalAlert[['timestamp', 'site', 'source', 'alert', 'updateTS']]
    alert_toDB(InternalAlert, 'site_level_alert', window, 'internal')
    
    SitePublicAlert = PublicAlert.loc[PublicAlert.site == site][['timestamp', 'site', 'source', 'alert', 'updateTS']]
    alert_toDB(SitePublicAlert, 'site_level_alert', window, 'public')
    
    GSMAlert = PublicAlert.loc[PublicAlert.site == site][['site', 'alert', 'palert_source']]
    public_CurrAlert = SitePublicAlert.alert.values[0]
    
    if public_CurrAlert != 'A0':
        if len(validity_A) == 0:
            GSMAlert.to_csv('GSMAlert.txt', header = False, index = None, sep = ':', mode = 'a')
        else:
            if public_PrevAlert != public_CurrAlert:
                GSMAlert.to_csv('GSMAlert.txt', header = False, index = None, sep = ':', mode = 'a')

    return PublicAlert

def writeAlertToDb(alertfile):
    f = open(alertfile,'r')
    alerttxt = f.read()
    f.close()

    sys.path.insert(0, '/home/dynaslope/Desktop/Senslope Server/')
    import senslopeServer as server

    server.writeAlertToDb(alerttxt)
    
def main():
    start = datetime.now()
    
    with open('GSMAlert.txt', 'w') as w:
        w.write('')
    
    window,config = rtw.getwindow()
    
    PublicAlert = pd.DataFrame({'timestamp': [window.end]*len(q.GetRainProps()), 'site': q.GetRainProps().name.values, 'source': ['public']*len(q.GetRainProps()), 'alert': [np.nan]*len(q.GetRainProps()), 'updateTS': [window.end]*len(q.GetRainProps()), 'palert_source': [np.nan]*len(q.GetRainProps()), 'internal_alert': [np.nan]*len(q.GetRainProps()), 'validity': [np.nan]*len(q.GetRainProps()), 'sensor_alert': [np.nan]*len(q.GetRainProps()), 'rain_alert': [np.nan]*len(q.GetRainProps())})
    PublicAlert = PublicAlert[['timestamp', 'site', 'source', 'alert', 'updateTS', 'palert_source', 'internal_alert', 'validity', 'sensor_alert', 'rain_alert']]

    Site_Public_Alert = PublicAlert.groupby('site')
    PublicAlert = Site_Public_Alert.apply(SitePublicAlert, window=window)
    PublicAlert = PublicAlert[['timestamp', 'site', 'alert', 'palert_source', 'internal_alert', 'validity', 'sensor_alert', 'rain_alert']]
    PublicAlert = PublicAlert.rename(columns = {'palert_source': 'source'})
    PublicAlert = PublicAlert.sort('site')
    print PublicAlert
    
    PublicAlert.to_csv('PublicAlert.txt', header=True, index=None, sep='\t', mode='w')
    
    dfjson = PublicAlert.to_json(orient="records", date_format="iso")
    dfjson = dfjson.replace('T', ' ').replace('.000Z', '')
    with open('PublicAlert.json', 'w') as w:
        w.write(dfjson)
            
    GSMAlert = pd.read_csv('GSMAlert.txt', sep = ':', header = None, names = ['site', 'alert', 'source'])
    if len(GSMAlert) != 0:
        with open('GSMAlert.txt', 'w') as w:
            w.write('As of ' + str(datetime.now())[:16] + '\n')
        GSMAlert.to_csv('GSMAlert.txt', header = False, index = None, sep = ':', mode = 'a')

        # write text file to db
#        writeAlertToDb('GSMAlert.txt')
    
    print "run time =", datetime.now() - start
    
    return PublicAlert

################################################################################

if __name__ == "__main__":
    main()