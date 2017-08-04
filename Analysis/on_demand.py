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

def WriteToDB(site_id, ts, alert):
    query = "SELECT * FROM site_level_alert WHERE source = 'public' and site = '%s' ORDER BY timestamp DESC LIMIT 1" %site_id
    prevpubdf = q.GetDBDataFrame(query)
    query = "SELECT * FROM site_level_alert WHERE source = 'internal' and site = '%s' ORDER BY timestamp DESC LIMIT 1" %site_id
    previnternaldf = q.GetDBDataFrame(query)
    prev_internal = previnternaldf.alert.values[0]
    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)

    if prevpubdf.alert.values[0] == 'A0':
        if alert == 'd1':
            pub_alert = 'A1'
            if prev_internal == 'A0':
                internal = 'A1-D'
            else:
                internal = 'ND-D'
        else:
            pub_alert = 'A2'
            if prev_internal == 'A0':
                internal = 'A2-M'
            else:
                internal = 'ND-M'
    
        pubdf = pd.DataFrame({'timestamp': [ts], 'site': [site_id], 'source':['public'], 'alert':[pub_alert], 'updateTS': [ts]})        
        pubdf.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                
    else:
        if prevpubdf.alert.values[0] == 'A1':
            if alert == 'd1':
                if 'D' not in prev_internal:
                    internal = prev_internal + 'D'
                else:
                    internal = prev_internal
            else:
                pub_alert = 'A2'
                if 'A1' in prev_internal:
                    internal = 'A2-M' + prev_internal[len(prev_internal)-2::]
                else:
                    internal = 'ND-M' + prev_internal[len(prev_internal)-2::]
                pubdf = pd.DataFrame({'timestamp': [ts], 'site': [site_id], 'source':['public'], 'alert':[pub_alert], 'updateTS': [ts]})        
                pubdf.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)

        else:
            internal = prev_internal[0:3]
            for i in ['S', 's', 'G', 'g']:
                if i in prev_internal:
                    internal += i
            if alert == 'd2' or 'M' in prev_internal:
                internal += 'M'
            for i in ['R', 'E']:
                if i in prev_internal:
                    internal += i
            if alert == 'd1' or 'D' in prev_internal:
                internal += 'D'
            for i in ['S0', 's0', 'G0', 'g0']:
                if i in prev_internal:
                    internal = internal.replace(i[0:1], i)

        d1df = pd.DataFrame({'timestamp': [ts], 'site': [site_id], 'source':['on demand'], 'alert':[alert], 'updateTS': [ts]})
        internaldf = pd.DataFrame({'timestamp': [ts], 'site': [site_id], 'source':['internal'], 'alert':[internal], 'updateTS': [ts]})

        d1df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        internaldf.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        
def main():
    
    while True:
        site_id = raw_input('site name: ').lower()
        df = q.GetRainProps('rain_props')
        df['name'] = df['name'].apply(lambda x: x.lower())
        if site_id in df.name.values:
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
        
    while True:
        ts = raw_input('request timestamp format YYYY-MM-DD HH:MM (e.g. 2017-11-07 13:30);\ndefault is current time (rounded-off to HH:30 or HH:00): ')
        if ts == '':
            ts = datetime.now()
            break
        else:
            try:
                ts = pd.to_datetime(ts)
                break
            except:
                print 'incorrect timestamp format'
                continue

    while True:
        alert = raw_input('operational trigger (d1/d2): ').lower()
        if alert in ['d1', 'd2']:
            break
        else:
            print 'invalid operational trigger'
            continue
    
    while True:
        requester = raw_input('Requested by (LEWC/LGU): ').upper()
        if requester in ['LEWC','LGU']:
            break
        else:
            continue
    
    reason = raw_input('Monitoring requested due to (e.g. heavy rainfall): ')
    site_info = raw_input('Current Site Info (e.g. rainfall is 80% of threshold): ')
    
    ts = adjust_time(ts)
    
    WriteToDB(site_id, ts, alert)
    
    df = pd.DataFrame({'ts':[ts], 'site_id':[site_id], 'alert':[alert], 'requester':[requester], 'reason':[reason], 'site_info':[site_info]})
    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
    df.to_sql(name = 'demand_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    
if __name__ == '__main__':
    main()