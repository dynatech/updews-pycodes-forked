import pandas as pd
from datetime import datetime
import querySenslopeDb as q

def uptime(upts, df):
    up_index = upts.index[0]
    updf = df[(df.timestamp <= upts['ts'].values[0])&(df.updateTS >= upts['ts'].values[0])]
    if len(updf) == 0:
        upts.loc[upts.index == up_index, ['status']] = 'down'
    else:
        upts.loc[upts.index == up_index, ['status']] = 'up'
    return upts

def main(start, end):
    
    query = "SELECT * FROM %s.site_level_alert where source = 'internal' and alert not like '%s' and \
        ((timestamp <= '%s' and updateTS >= '%s') or (timestamp >= '%s' and timestamp <= '%s') \
        or (updateTS >= '%s' and updateTS <= '%s'))" %(q.Namedb, 'ND%', start, end, start, end, start, end)
    df = q.GetDBDataFrame(query)

    rangeTS = pd.date_range(start='2017-01-01', end = '2017-04-01', freq='30min')
    rangeTS = rangeTS[0:-1]
    pub_uptime = pd.DataFrame({'ts':rangeTS, 'status':['-']*len(rangeTS)})
    pub_uptimeTS = pub_uptime.groupby('ts')
    pub_uptime = pub_uptimeTS.apply(uptime, df=df)
    
    percent_up = 100 - (100. * len(pub_uptime[pub_uptime.status == 'down'])/len(pub_uptime))
    
    return percent_up

if __name__ == '__main__':
    start = datetime.now()
    percent_up = main(start = '2017-01-01', end='2017-04-01')
    print '\n\n'
    print 'alert uptime = ' + str(np.round(percent_up, 2)) + '%'
    print '\n\n'
    print 'runtime =', str(datetime.now() - start)