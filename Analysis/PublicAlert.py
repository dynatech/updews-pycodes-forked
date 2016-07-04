import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pandas.stats.api import ols
from sqlalchemy import create_engine
import sys

import cfgfileio as cfg
import rtwindow as rtw
import querySenslopeDb as q

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
    
    print df
    
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
    query = "SELECT * FROM ( SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' " %site
    if site == 'bto':
        query += "or site = 'bat' "
    elif site == 'mng':
        query += "or site = 'man' "
    elif site == 'png':
        query += "or site = 'pan' "
    elif site == 'jor':
        query += "or site = 'pob' "
    query += "ORDER BY timestamp DESC) AS sub GROUP BY source"
    
    site_alert = q.GetDBDataFrame(query)
    
    if 'L3' in site_alert.alert.values:
        public_alert = 'A3'
    elif 'L2' in site_alert.alert.values:
        public_alert = 'A2'
    elif 'r1' in site_alert.alert.values or 'd1' in site_alert.alert.values or 'd1' in site_alert.alert.values:
        public_alert = 'A1'
    else:
        public_alert = 'A0'
    
    alert_index = PublicAlert.loc[PublicAlert.site == site].index[0]
    PublicAlert.loc[alert_index] = [window.end, PublicAlert.site.values[0], 'public', public_alert, window.end]
    
    SitePublicAlert = PublicAlert.loc[PublicAlert.site == site]
    
    alert_toDB(SitePublicAlert, 'site_level_alert', window)

    return PublicAlert
    
def main():
    start = datetime.now()
    
    window,config = rtw.getwindow()
    PublicAlert = pd.DataFrame({'timestamp': [window.end]*len(q.GetRainProps()), 'site': q.GetRainProps().name.values, 'source': ['public']*len(q.GetRainProps()), 'alert': [np.nan]*len(q.GetRainProps()), 'updateTS': [window.end]*len(q.GetRainProps())})
    PublicAlert = PublicAlert[['timestamp', 'site', 'source', 'alert', 'updateTS']]
    
    Site_Public_Alert = PublicAlert.groupby('site')
    
    PublicAlert = Site_Public_Alert.apply(SitePublicAlert, window=window)
    
    print PublicAlert
    
    print "run time =", datetime.now() - start

    return PublicAlert

################################################################################

if __name__ == "__main__":
    main()