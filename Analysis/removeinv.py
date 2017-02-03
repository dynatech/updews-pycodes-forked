import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import querySenslopeDb as q

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
        query = "DELETE FROM site_level_alert where site = '%s' and source = 'internal' and alert like '%s'" %(df['site'].values[0], df['alert'].values[0] + '%')
        query += " and timestamp = '%s'" %ts
        cur.execute(query)
        db.commit()
        db.close()
    except:
        pass

def currentinv(df, withalert):
    level = df['alert'].values[0]
    if level == 'A3':
        alert = withalert.loc[withalert.alert == 'A3']
        df = df[df.site.isin(alert.site)]
    elif level == 'A2':
        alert = withalert.loc[withalert.alert == 'A2']
        df = df[df.site.isin(alert.site)]
    else:
        df = df[df.site.isin(withalert.site)]
    return df

def main_inv(ts=datetime.now()):
    # sites with invalid alert
    query = "SELECT * FROM smsalerts where ts_set >= '%s' and alertstat = 'invalid'" %(pd.to_datetime(ts) - timedelta(10))
    df = q.GetDBDataFrame(query)
    dfid = df.groupby('alert_id')
    alertdf = dfid.apply(alertmsg)
    alertdf = alertdf.reset_index(drop=True)
    alertdf = alertdf.loc[(alertdf.alert != 'l0t')]

    # remove invalid public and internal alert in db
    invalertdf = alertdf.loc[alertdf.timestamp >= ts - timedelta(hours=3)]
    invalertdf = invalertdf[~(invalertdf.source.str.contains('sensor'))]
    invalertdf = invalertdf.loc[(invalertdf.alert != 'A1')]
    sitealertdf = invalertdf.groupby('site')
    sitealertdf.apply(removeinvpub)

    # write site with current invalid alert to InvalidAlert.txt
    allpub = pd.read_csv('PublicAlert.txt', sep = '\t')
    withalert = allpub.loc[(allpub.alert != 'A0')]
    alertdf = alertdf[['site', 'alert', 'timestamp', 'iomp', 'remarks']]
    alertdf = alertdf.sort_values('timestamp', ascending = False)
    alertdf = alertdf.drop_duplicates(['site', 'alert'])
    alertdflevel = alertdf.groupby('alert')
    finaldf = alertdflevel.apply(currentinv, withalert=withalert)
    finaldf.to_csv('InvalidAlert.txt', sep=':', header=True, index=False, mode='w')

def main_l0t(ts=datetime.now()):
    query = "SELECT * FROM smsalerts where ts_ack >= '%s' and alertstat = 'valid' and alertmsg like '%s'" %(pd.to_datetime(ts) - timedelta(hours=1), '%l0t%')
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

if __name__ == '__main__':
    start = datetime.now()
    main_l0t()
    main_inv()
    print 'runtime =', str(datetime.now() - start)