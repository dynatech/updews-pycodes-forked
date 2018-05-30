import pandas as pd
from datetime import date, time, datetime, timedelta
from sqlalchemy import create_engine

import querySenslopeDb as q
import filepath

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

def alertmsg(df):
    alert_msg = df['alertmsg'].values[0].split('\n')
    ts = pd.to_datetime(alert_msg[0].replace('As of ',''))
    site = alert_msg[1].split(':')[0]
    alert = alert_msg[1].split(':')[1]
    if 'sensor' in alert_msg[1]:
        source = 'sensor'
    else:
        source = alert_msg[1].split(':')[2]
    iomp = df['ack'].values[0]
    remarks = df['remarks'].values[0]
    alertdf = pd.DataFrame({'timestamp': [ts], 'site': [site], 'alert': [alert], 'source': [source], 'iomp': [iomp], 'remarks': [remarks]})
    if len(alertdf) == 0:
        alertdf = pd.DataFrame({'timestamp': [], 'site': [], 'alert': [], 'source': [], 'iomp': [], 'remarks': []})
    return alertdf

def removeinvpub(df):
    try:
        ts = pd.to_datetime(df['timestamp'].values[0])
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "SELECT * FROM (SELECT * FROM site_level_alert WHERE site = '%s' and source = 'public' and alert like '%s' and timestamp >= '%s' and updateTS <= '%s' order by timestamp desc) AS sub GROUP BY source" %(df['site'].values[0], df['alert'].values[0] + '%', ts.date(), ts + timedelta(hours=4))
        df = q.GetDBDataFrame(query)
        
        ts = pd.to_datetime(df['timestamp'].values[0])
        query = "DELETE FROM site_level_alert where site = '%s' and source = 'public' and alert = '%s'" %(df['site'].values[0], df['alert'].values[0])
        query += " and timestamp = '%s'" %ts
        cur.execute(query)
        db.commit()
        db.close()
    except:
        pass

def currentinv(withalert, df):
    site = withalert['site'].values[0]
    
    query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'public' AND alert != 'A0' ORDER BY timestamp DESC LIMIT 3" %site
    prev_PAlert = q.GetDBDataFrame(query)
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

    invdf = df[(df.site == site)&(df.timestamp >= start_monitor)]

    return invdf

def main_inv(ts):
    # sites with invalid alert
    query = "SELECT * FROM smsalerts where ts_set >= '%s' and alertstat = 'invalid'" %(pd.to_datetime(ts) - timedelta(300))
    df = q.GetDBDataFrame(query)
    
    # wrong format in db
    if 409 in df['alert_id'].values:
        alertmsg409 = 'As of 2017-02-03 09:36\nosl:A2:ground'
        df.loc[df['alert_id'] == 409, ['alertmsg']] = alertmsg409
    if 408 in df['alert_id'].values:
        alertmsg408 = 'As of 2017-02-02 20:11\npla:A2:"sensor(plat:20)"'
        df.loc[df['alert_id'] == 408, ['alertmsg']] = alertmsg408
    
    dfid = df.groupby('alert_id')
    alertdf = dfid.apply(alertmsg)
    alertdf = alertdf.reset_index(drop=True)
    alertdf = alertdf.loc[(alertdf.alert != 'l0t')]
    
    # remove invalid public alert in db
    invalertdf = alertdf.loc[alertdf.timestamp >= ts - timedelta(hours=3)]
    invalertdf = invalertdf[~(invalertdf.source.str.contains('sensor'))]
    invalertdf = invalertdf.loc[(invalertdf.alert != 'A1')]
    sitealertdf = invalertdf.groupby('site')
    sitealertdf.apply(removeinvpub)

    # write site with current invalid alert to InvalidAlert.txt
    query = "(SELECT * FROM (SELECT * FROM site_level_alert WHERE source = 'public' \
        AND alert != 'A0' AND updateTS >= '%s' ORDER BY timestamp DESC) AS SUB GROUP BY site)" %(ts - timedelta(hours=0.5))
    withalert = q.GetDBDataFrame(query)
    sitewithalert = withalert.groupby('site')
    alertdf = alertdf[alertdf.site.isin(withalert['site'].values)]
    alertdf = alertdf[alertdf.source != 'ground']
    finaldf = sitewithalert.apply(currentinv, df=alertdf)
    try:
        finaldf = finaldf.sort('timestamp', ascending=False).drop_duplicates(['site','source'], keep='first').reset_index(drop='True')
    except:
        finaldf = pd.DataFrame(columns = ['alert', 'iomp', 'remarks', 'site', 'source', 'timestamp'])
    file_path = filepath.output_file_path('all', 'public')['monitoring_output']
    finaldf.to_csv(file_path+'InvalidAlert.txt', sep=':', header=True, index=False, mode='w')
    return finaldf

def main_l0t(ts):
    query = "SELECT * FROM smsalerts where ts_ack >= '%s' and alertstat = 'valid' and alertmsg like '%s'" %(pd.to_datetime(ts) - timedelta(hours=4), '%l0t%')
    df = q.GetDBDataFrame(query)

    if len(df) != 0:
        dfid = df.groupby('alert_id')
        alertdf = dfid.apply(alertmsg)
        alertdf = alertdf.reset_index(drop=True)
    
        if len(alertdf) != 0:
            sites = str(list(alertdf.site.values)).replace('[', '(').replace(']', ')')
            query = "SELECT * FROM (SELECT * FROM site_level_alert WHERE site in %s AND alert = 'l0t' ORDER BY timestamp DESC) AS SUB GROUP BY site" %sites
            df = q.GetDBDataFrame(query)
            
            df['alert'] = 'l2'
        
            engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
            
            for i in range(len(df)):
                try:
                    df[i:i+1].to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                except:
                    pass

def main():
    start = datetime.now()
    print start
    
    ts = round_data_time(start)
    main_l0t(ts)
    main_inv(ts)
    
    print 'runtime =', str(datetime.now() - start)

if __name__ == '__main__':
    main()    