import os
import sys
import pandas as pd
import numpy as np

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../updews-pycodes/Analysis/'))
if not path in sys.path:
   sys.path.insert(1, path)
del path

import querySenslopeDb as q

def gndmeas_id(df, gndmeas_table):
   gndmeas_table[df['timestamp'].values[0]][df['crack_id'].values[0]] = df['meas'].values[0]
   return gndmeas_table

def gndmeas(df, gndmeas_table):
   dfid = df.groupby('crack_id')
   gndmeas_table = dfid.apply(gndmeas_id, gndmeas_table=gndmeas_table)
   return gndmeas_table

def gndmeas_vel(df):
    gndmeas_vel = df.sort_index()
    ts = df.columns
    gndmeas_vel[ts[0]] = [np.nan]*len(df)
    for i in range(1, len(ts)):
        gndmeas_vel[ts[i]] = abs(np.round(list((df[ts[i-1]] - df[ts[i]]) / ((ts[i-1] - (ts[i]))/np.timedelta64(1,'D'))), 2))
    return gndmeas_vel

site = sys.argv[1]
query = "SELECT * FROM senslopedb.gndmeas WHERE site_id = '%s' ORDER BY timestamp DESC LIMIT 200" %site
df = q.GetDBDataFrame(query)
df['timestamp'] = pd.to_datetime(df['timestamp'])

last10ts = sorted(set(df.timestamp.values), reverse=True)
if len(last10ts) > 11:
   last10ts = last10ts[0:11]
df = df[df.timestamp.isin(last10ts)]

dfts = df.groupby('timestamp')
gndmeas_table = pd.DataFrame(columns = sorted(last10ts), index=sorted(set(df.crack_id.values)))
gndmeas_table = dfts.apply(gndmeas, gndmeas_table=gndmeas_table)
gndmeas_table = gndmeas_table.reset_index(level=1, drop=True).reset_index()
gndmeas_table['crack_id'] = gndmeas_table['level_1']
gndmeas_table = gndmeas_table.set_index('crack_id')[sorted(last10ts)]
gndmeas_table = gndmeas_table[len(gndmeas_table.index) - len(set(gndmeas_table.index)) : len(gndmeas_table.index)]
gndmeas_alert = gndmeas_vel(gndmeas_table)

dfajson = gndmeas_alert.reset_index().to_json(orient='records',date_format='iso')
dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
print dfajson