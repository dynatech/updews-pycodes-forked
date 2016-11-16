import pandas as pd
from datetime import datetime, timedelta

import querySenslopeDb as q

def invalert(df):
    inv_alert = df['alertmsg'].values[0].split('\n')
    ts = pd.to_datetime(inv_alert[0].replace('As of ',''))
    site = inv_alert[1][0:3]
    alert = inv_alert[1][4:6]
    source = inv_alert[1][7:len(inv_alert[1])]
    iomp = df['ack'].values[0]
    remarks = df['remarks'].values[0]
    alertdf = pd.DataFrame({'timestamp': [ts], 'site': [site], 'alert': [alert], 'source': [source], 'iomp': [iomp], 'remarks': [remarks]})
    if len(alertdf) == 0:
        alertdf = pd.DataFrame({'timestamp': [], 'site': [], 'alert': [], 'source': [], 'iomp': [], 'remarks': []})
    return alertdf

def removeinvpub(df):
    ts = pd.to_datetime(df['timestamp'].values[0])
    db, cur = q.SenslopeDBConnect(q.Namedb)
    query = "DELETE FROM site_level_alert where site = '%s' and source = 'public' and alert = '%s'" %(df['site'].values[0], df['alert'].values[0])
    query += " and timestamp <= '%s' and updateTS >= '%s'" %(ts+timedelta(hours=0.5), ts-timedelta(hours=0.5))
    cur.execute(query)
    db.commit()
    query = "DELETE FROM site_level_alert where site = '%s' and source = 'internal' and alert like '%s'" %(df['site'].values[0], df['alert'].values[0] + '%')
    query += " and timestamp <= '%s' and updateTS >= '%s'" %(ts+timedelta(hours=0.5), ts-timedelta(hours=0.5))
    cur.execute(query)
    db.commit()
    db.close()

def invsensor(df):
    if 'sensor' in df['source'].values[0]:
        df['remarks'] = df['remarks'].apply(lambda x: x + ' [invalid sensor retrigger until ' + str(pd.to_datetime(df['timestamp'].values[0]) + timedelta(hours=7)) + ']')
    return df

def main(ts=datetime.now()):
    query = "SELECT * FROM smsalerts where ts_set >= '%s' and alertstat = 'invalid'" %(pd.to_datetime(ts) - timedelta(10))
    df = q.GetDBDataFrame(query)
    
    dfid = df.groupby('alert_id')
    alertdf = dfid.apply(invalert)
    alertdf = alertdf.reset_index(drop=True)

    invalertdf = alertdf.loc[alertdf.timestamp >= ts - timedelta(hours=3)]
    invalertdf = invalertdf[~(invalertdf.source.str.contains('sensor'))]
    invalertdf = invalertdf.loc[invalertdf.alert != 'A1']

    sitealertdf = invalertdf.groupby('site')
    sitealertdf.apply(removeinvpub)

    allpub = pd.read_csv('PublicAlert.txt', sep = '\t')
    withalert = allpub.loc[allpub.alert != 'A0'].site
    alertdf = alertdf[alertdf.site.isin(withalert)]
    sitealertdf = alertdf.groupby('site')
    alertdf = sitealertdf.apply(invsensor)
    alertdf = alertdf[['site', 'alert', 'timestamp', 'iomp', 'remarks']]
    alertdf.to_csv('InvalidAlert.txt', sep=':', header=True, index=False, mode='w')

if __name__ == '__main__':
    start = datetime.now()
    main()
    print 'runtime =', str(datetime.now() - start)