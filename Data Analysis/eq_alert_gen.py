import numpy as np
import pandas as pd
import sys
from datetime import datetime
from sqlalchemy import create_engine

from querySenslopeDb import *

sys.path.insert(0, '/home/dynaslope/Desktop/Senslope Server')
from senslopedbio import *
from senslopeServer import *


def getCritDist(mag):
    return (29.027 * (mag**2)) - (251.89*mag) + 547.97

def getrowDistancetoEQ(df):#,eq_lat,eq_lon):   
    dlon=eq_lon-df.lon
    dlat=eq_lat-df.lat
    dlon=np.radians(dlon)
    dlat=np.radians(dlat)
    a=(np.sin(dlat/2))**2 + ( np.cos(np.radians(eq_lat)) * np.cos(np.radians(df.lat)) * (np.sin(dlon/2))**2 )
    c= 2 * np.arctan2(np.sqrt(a),np.sqrt(1-a))
    d= 6371 * c
    return d

def getEQ():    
    query = """ SELECT * FROM %s.earthquake order by timestamp desc limit 1 """ % (Namedb)
    dfeq =  GetDBDataFrame(query)
    return dfeq.mag[0],dfeq.lat[0],dfeq.longi[0],dfeq.timestamp[0]

def getSites():
    query = """ SELECT * FROM %s.site_column """ % (Namedb)
    df = GetDBDataFrame(query)
    return df[['name','lat','lon']]

def uptoDB(df):
    engine=create_engine('mysql://root:senslope@192.168.1.102:3306/senslopedb')
    df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = Namedb, index = True)

#MAIN

mag,eq_lat,eq_lon,ts = getEQ()

#mag, eq_lat, eq_lon, ts = 5.2, 11.02,124.68,datetime.datetime.now()

critdist = getCritDist(mag)

print mag
if mag >=4:
    sites = getSites()
    dfg = sites.groupby('name')
    dist = dfg.apply(getrowDistancetoEQ)
    crits = dist[dist<critdist]
    crits = crits.reset_index()

    if len(crits.name.values) > 0:
        message = "EQALERT\nAs of %s: \nE1: %s" % (str(ts),','.join(str(n) for n in crits.name.values))
        print message
        WriteEQAlertMessageToDb(message)
    
        crits['timestamp']  = ts
        crits['alert'] = 'e1'
        crits['updateTS'] = ts
        crits['source'] = 'eq'
        crits['name'] = crits['name'].str[:3]
        
        crits = crits.drop_duplicates('name')
        crits.rename(columns = {'name':'site'}, inplace = True)
        crits = crits[['timestamp','site','source','alert','updateTS']].set_index('timestamp')
    
        uptoDB(crits)
    
    else:
        print "> No affected sites."

else:
    print '> Magnitude too small.'
    pass