import pandas as pd
import numpy as np
import outputFilteredData as ofd
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
    
def getDF():

    site = sys.argv[1]

    #time start
    timeStart = (dt.now()-td(2)).strftime("%y/%m/%d %H:%M:%S")
    timeEnd = (dt.now()).strftime("%y/%m/%d %H:%M:%S")
    timeTarget = (dt.now()-td(days=1,hours=6)).strftime("%Y-%m-%d %H:%M:%S")
    #print "End Time: " + timeEnd + ", Target Time: " + timeTarget

    #get the max number of nodes for the site
    engineRaw = create_engine('mysql+mysqldb://updews:october50sites@127.0.0.1/senslopedb')
    query = "SELECT num_nodes FROM site_column_props WHERE name = '%s'" % (site)
    results = pd.io.sql.read_sql(query,engineRaw)
    maxNode = int(results.ix[0]["num_nodes"])

    allNodes = []

    for num in range(1, maxNode):
        #print "current node: %d" % (num)

        #Changed date difference is 1 day or 24 hours
        df = ofd.getFilteredData(isCmd = False, inSite = site, inNode = num, inStart = timeTarget)
        
        if df.empty:
            #for times that there are no data for the specific node
            continue

        #site should be "id". Its only "site" temporarily...
        df.columns = ['timestamp','site','xalert','yalert','zalert']
        df = df.set_index(['timestamp'])
        
        df2 = df.copy()
        dfa = []


        df3 = df2.resample('30Min').fillna(method='pad')
        #dfv = df3 - df3.shift(12)

        dfv = df3.copy()
        dfv.xalert = abs(df3.xalert - df3.xalert.shift(12))
        dfv.yalert = abs(df3.yalert - df3.yalert.shift(12))
        dfv.zalert = abs(df3.zalert - df3.zalert.shift(12))

        #Enable this for debugging
        #print dfv

        if len(dfa) == 0:
            dfa = dfv.copy()
        else:
            dfa = dfa.append(dfv)
                 
        dfa = dfa[pd.notnull(dfa.xalert)]

        if len(allNodes) == 0:
            allNodes = dfa.copy()
        else:
            allNodes = allNodes.append(dfv)
    
    allNodes = allNodes[pd.notnull(allNodes.xalert)]

    allNodes = allNodes[allNodes.index > dt.now()-td(hours=23, minutes=30)]

    dfajson = allNodes.reset_index().to_json(orient="records",date_format='iso')
    dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
    print dfajson
getDF();
