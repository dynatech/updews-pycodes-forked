import numpy as np
import pandas as pd
import sys

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


#MAIN

mag,eq_lat,eq_lon,ts = getEQ()

critdist = getCritDist(mag)
#critdist = 100

sites = getSites()
dfg = sites.groupby('name')
dist = dfg.apply(getrowDistancetoEQ)
crits = dist[dist<critdist]

crits = crits.reset_index()

if len(crits.name.values) > 0:
    message = "EQALERT\nAs of %s: \nE1: %s" % (str(ts),','.join(str(n) for n in crits.name.values))
    print message
    WriteOutboxMessageToDb(message,recepients,send_status='UNSENT')

else:
    print "No affected sites."

