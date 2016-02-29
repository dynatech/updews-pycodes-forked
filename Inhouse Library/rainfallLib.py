import os
import sys
import pandas as pd
import numpy as np
import time
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
#import querySenslopeDb as qs

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../Data Analysis'))
if not path in sys.path:
    sys.path.insert(1,path)
del path    

#import Data Analysis/querySenslopeDb
import querySenslopeDb as qs
    
#download the NOAH Rainfall data directly from ASTI
def downloadRainfallNOAH(rsite, fdate, tdate):   
    #Reduce latestTS by 1 day as a work around for NOAH's API of returning data
    #   that starts from 8am
    fdateMinus = (pd.to_datetime(fdate) - td(1)).strftime("%Y-%m-%d")
    
    url = "http://weather.asti.dost.gov.ph/home/index.php/api/data/%s/from/%s/to/%s" % (rsite,fdateMinus,tdate)
    r = requests.get(url)

    try:
        df = pd.DataFrame(r.json()["data"])
    except TypeError:
        print "    No device with id of %s" % rsite
        return pd.DataFrame()

    try:
        df = df.set_index(['dateTimeRead'])
        df.index = pd.to_datetime(df.index)
        df = df["rain_value"].astype(float)
        df = df.resample('15Min').fillna(0.00)
        dfs = pd.rolling_sum(df,96)
        dfa = pd.DataFrame({"rval":df,"cumm":dfs})
        dfa = dfa.fillna(0)
        dfa = dfa[96:]
        
        #drop the dataframe duplicates
        dfa = dfa.drop_duplicates()        
        
        #remove the entries that are less than or equal to fdate
        dfa = dfa[dfa.index > fdate]            
        
        #rename the "index" into "timestamp"
        dfa.index.names = ["timestamp"]
        
        return dfa
        
    except:
        return pd.DataFrame()
        
def downloadRainfallNOAHJson(rsite, fdate, tdate):
    dfa = downloadRainfallNOAH(rsite, fdate, tdate)
    
    if dfa.empty: 
        return pd.DataFrame()
    else:        
        
        dfajson = dfa.reset_index().to_json(orient="records",date_format='iso')
        dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
        #print dfajson
        return dfajson
        
#insert the newly downloaded data to the database
def updateRainfallNOAHTableData(rsite, fdate, tdate):
    noahData = downloadRainfallNOAH(rsite, fdate, tdate)
    #print noahData
    
    curTS = time.strftime("%Y-%m-%d")  
    
    table_name = "rain_noah_%s" % (rsite)
    
    if noahData.empty: 
        print "    no data..."
        
        #The table is already up to date
        if tdate > curTS:
            return 
        else:
            #Insert an entry with values: [timestamp,-1,-1] as a marker
            #   for the next time it is used
            #   note: values with -1 should not be included in values used for computation
            placeHolderData = pd.DataFrame({"timestamp": tdate+" 00:00:00","cumm":-1,"rval":-1}, index=[0])
            placeHolderData = placeHolderData.set_index(['timestamp'])
            #print placeHolderData
            qs.PushDBDataFrame(placeHolderData, table_name) 
            
            #call this function again until the maximum recent timestamp is hit        
            updateNOAHSingleTable(rsite)

    else:        
        #Insert the new data on the noahid table
        qs.PushDBDataFrame(noahData, table_name) 
        
        #The table is already up to date
        if tdate > curTS:
            return         
        else:
            #call this function again until the maximum recent timestamp is hit        
            updateNOAHSingleTable(rsite)
    
    
def getRainfallNOAHcmd():
    #first argument is the rain gauge id
    rsite = sys.argv[1]
    
    # adjust fdate to start 1 day later
    fdate = (pd.to_datetime(sys.argv[2]) - td(1)).strftime("%Y-%m-%d")
    
    tdate = sys.argv[3]

    #download rainfall data from the NOAH website
    downloadRainfallNOAH(rsite, fdate, tdate)

 
def doesNOAHTableExist(noahid):
    table_name = "rain_noah_%s" % (noahid)
    exists = qs.DoesTableExist(table_name)

    if exists:
        print table_name + " Exists!"
    else:
        print table_name + " DOES NOT exist..."
    
    return exists
    
#Create the
def createNOAHTable(noahid):
    #Create table for noahid before proceeding with the download
    query = "CREATE TABLE `senslopedb`.`rain_noah_%s` (" % noahid
    query = query + "    `timestamp` DATETIME NOT NULL,"
    query = query + "    `cumm` FLOAT NOT NULL,"
    query = query + "    `rval` FLOAT NOT NULL,"
    query = query + "    PRIMARY KEY (`timestamp`))"
    query = query + "ENGINE = InnoDB "
    query = query + "DEFAULT CHARACTER SET = utf8 "
    query = query + "COMMENT = 'Downloaded Rainfall Data from NOAH Rain Gauge ID %s'" % noahid
    #print query

    print "Creating table: rain_noah%s..." % noahid

    #Create new table
    qs.ExecuteQuery(query)
    
def updateNOAHSingleTable(noahid):
    #check if table "rain_noah_" + "noahid" exists already
    if doesNOAHTableExist(noahid) == False:
        #Create a NOAH table if it doesn't exist yet
        createNOAHTable(noahid)
    
    #Find the latest timestamp for noahid (which is also the start date)
    table_name = "rain_noah_%s" % (noahid)
    latestTS = qs.GetLatestTimestamp2(table_name)    
    
    if (latestTS == '') or (latestTS == None):
        #assign a starting date if table is currently empty
        latestTS = "2014-01-01 00:00:00"
    else:
        latestTS = latestTS.strftime("%Y-%m-%d %H:%M:%S")
    
    print "    Start timestamp: " + latestTS
    
    #Generate end time    
    endTS = (pd.to_datetime(latestTS) + td(50)).strftime("%Y-%m-%d")
    print "    End timestamp: %s" % (endTS)
    
    #Download data for noahid
    updateRainfallNOAHTableData(noahid, latestTS, endTS)    

    
def updateNOAHTables():
    #get the list of rainfall NOAH rain gauge IDs
    dfRain = qs.GetRainNOAHList()

    for noahid in dfRain:
        updateNOAHSingleTable(noahid)

#updateNOAHTables()
#updateNOAHSingleTable(813)