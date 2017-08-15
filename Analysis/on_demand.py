import pandas as pd
from datetime import datetime, date, time
from sqlalchemy import create_engine

import querySenslopeDb as q

def adjust_time(endpt):
    endpt = pd.to_datetime(endpt)
    end_Year=endpt.year
    end_month=endpt.month
    end_day=endpt.day
    end_hour=endpt.hour
    end_minute=endpt.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))
    return end

def WriteToDB(site):
    ts = datetime.now()
    ts = adjust_time(ts)
    query = "SELECT * FROM site_level_alert WHERE source = 'public' and site = '%s' ORDER BY timestamp DESC LIMIT 1" %site
    prevpubdf = q.GetDBDataFrame(query)
    query = "SELECT * FROM site_level_alert WHERE source = 'internal' and site = '%s' ORDER BY timestamp DESC LIMIT 1" %site
    previnternaldf = q.GetDBDataFrame(query)
    internal = previnternaldf.alert.values[0]
    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
    if prevpubdf.alert.values[0] == 'A0':
        d1df = pd.DataFrame({'timestamp': [ts], 'site': [site[0:3]], 'source':['on demand'], 'alert':['d1'], 'updateTS': [ts]})
        pubdf = pd.DataFrame({'timestamp': [ts], 'site': [site[0:3]], 'source':['public'], 'alert':['A1'], 'updateTS': [ts]})
        internaldf = pd.DataFrame({'timestamp': [ts], 'site': [site[0:3]], 'source':['internal'], 'alert':[internal + '-D'], 'updateTS': [ts]})
        
        pubdf.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        d1df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        internaldf.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    else:
        d1df = pd.DataFrame({'timestamp': [ts], 'site': [site[0:3]], 'source':['on demand'], 'alert':['d1'], 'updateTS': [ts]})
        internaldf = pd.DataFrame({'timestamp': [ts], 'site': [site[0:3]], 'source':['internal'], 'alert':[internal + 'D'], 'updateTS': [ts]})
        
        d1df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        internaldf.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        
def main():
    
    while True:
        site = raw_input('site name: ').lower()
        df = q.GetRainProps('rain_props')
        df['name'] = df['name'].apply(lambda x: x.lower())
        if site in df.name.values:
            break
        else:
            print 'site name is not in the list'
            print '#'*45
            print '##', ','.join(df.name.values[0:10]), '##'
            print '##', ','.join(df.name.values[10:20]), '##'
            print '##', ','.join(df.name.values[20:30]), '##'
            print '##', ','.join(df.name.values[30:40]), '##'
            print '##', ','.join(df.name.values[40:50]), '##'
            print '#'*45
            continue
    
    WriteToDB(site)
    
if __name__ == '__main__':
    main()