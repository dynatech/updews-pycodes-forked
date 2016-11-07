import pandas as pd
from datetime import datetime, timedelta

import querySenslopeDb as q

def invalert(df):
    inv_alert = df['alertmsg'].values[0].split('\n')
    ts = inv_alert[0].replace('As of ','')
    site = inv_alert[1][0:3]
    alert = inv_alert[1][4:6]
    source = inv_alert[1][7:len(inv_alert[1])]
    alertdf = pd.DataFrame({'ts': [ts], 'site': [site], 'alert': [alert], 'source': [source]})
    return alertdf

def removeinvpub(df):
    ts = pd.to_datetime(df.ts[0])
    db, cur = q.SenslopeDBConnect(q.Namedb)
    query = "DELETE FROM site_level_alert where site = '%s' and source = 'public' and alert = '%s' and timestamp <= '%s' \
            and updateTS >= '%s'" %(df.site[0], df.alert[0], ts+timedelta(hours=0.5), ts-timedelta(hours=0.5))
    cur.execute(query)
    db.commit()
    db.close()

def main(ts=datetime.now()):
    query = "SELECT * FROM smsalerts where ts_set >= '%s'" %(pd.to_datetime(ts) - timedelta(hours=0.5))
    df = q.GetDBDataFrame(query)
    df = df.loc[df.alertstat == 'invalid']
    
    dfid = df.groupby('alert_id')
    alertdf = dfid.apply(invalert)
    alertdf = alertdf.reset_index(drop=True)
    
    sitealertdf = alertdf.groupby('site')
    sitealertdf.apply(removeinvpub)

if __name__ == '__main__':
    start = datetime.now()
    main()
    print 'runtime =', str(datetime.now() - start)