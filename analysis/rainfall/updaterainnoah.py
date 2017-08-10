from datetime import datetime, timedelta
import os
import pandas as pd
import requests
import sys

import rainfall as rain

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as qdb
    
#download the NOAH Rainfall data directly from ASTI
def download_rainfall_noah(noah_id, fdate, tdate):   
    #Reduce latestTS by 1 day as a work around for NOAH's API of returning data
    #   that starts from 8am
    #Reduce by another 1 day due to the "rolling_sum" function
    fdateMinus = (pd.to_datetime(fdate) - timedelta(1)).strftime("%Y-%m-%d")
    
    url = "http://weather.asti.dost.gov.ph/web-api/index.php/api/data/%s/from/%s/to/%s" % (noah_id,fdateMinus,tdate)
    try:
        req = requests.get(url, auth=('phivolcs.ggrdd', 'PhiVolcs0117'))
    except:
        qdb.print_out("    Can not get request. Please check if your internet connection is stable")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(req.json()["data"])
    except:
        qdb.print_out("    error: %s" % noah_id)
        return pd.DataFrame()

    try:
        #rename "dateTimeRead" into "ts" and "rain_value" into "rain"
        df = df.rename(columns = {'rain_value': 'rain', 'dateTimeRead': 'ts'})
        
        df = df.drop_duplicates('ts')
        df['ts'] = df['ts'].apply(lambda x: pd.to_datetime(str(x)[0:19]))
        df = df.set_index(['ts'])
        df = df.sort_index()
        
        #remove the entries that are less than fdate
        df = df[df.index > fdate]            
        
        return df[['rain']]
        
    except:
        return pd.DataFrame()
        
#insert the newly downloaded data to the database
def update_table_data(noah_id, gauge_name, fdate, tdate, noah_gauges):
    noahData = download_rainfall_noah(noah_id, fdate, tdate)
    curTS = datetime.now()
    
    if noahData.empty: 
        qdb.print_out("    no data...")
        
        #The table is already up to date
        if pd.to_datetime(tdate) > curTS:
            return 
        else:
            #Insert an entry with values: [timestamp,-1] as a marker
            #   for the next time it is used
            #   note: values with -1 should not be included in values used for computation
            placeHolderData = pd.DataFrame({"ts": tdate+" 00:00:00","rain":-1}, index=[0])
            placeHolderData = placeHolderData.set_index('ts')
            qdb.push_db_dataframe(placeHolderData, gauge_name) 
            
            #call this function again until the maximum recent timestamp is hit        
            update_single_table(noah_gauges)

    else:        
        #Insert the new data on the noahid table
        noahData = noahData.reset_index()
        noahData = noahData.drop_duplicates('ts')
        noahData = noahData.set_index('ts')
        qdb.push_db_dataframe(noahData, gauge_name)
        
        #The table is already up to date
        if pd.to_datetime(tdate) > curTS:
            return         
        else:
            #call this function again until the maximum recent timestamp is hit        
            update_single_table(noah_gauges)
    
#Create NOAH Table
def createNOAHTable(gauge_name):
    #Create table for noahid before proceeding with the download
    query = "CREATE TABLE `%s` (" %gauge_name
    query += "  `data_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL,"
    query += "  `rain` DECIMAL(4,1) NOT NULL,"
    query += "  `temperature` DECIMAL(3,1) NULL DEFAULT NULL,"
    query += "  `humidity` DECIMAL(3,1) NULL DEFAULT NULL,"
    query += "  `battery1` DECIMAL(4,3) NULL DEFAULT NULL,"
    query += "  `battery2` DECIMAL(4,3) NULL DEFAULT NULL,"
    query += "  `csq` TINYINT(3) NULL DEFAULT NULL,"
    query += "  PRIMARY KEY (`data_id`),"
    query += "  UNIQUE INDEX `ts_UNIQUE` (`ts` ASC))"
    query += " ENGINE = InnoDB"
    query += " DEFAULT CHARACTER SET = utf8;"

    qdb.print_out("Creating table: %s..." % gauge_name)

    #Create new table
    qdb.execute_query(query)

def GetLatestTimestamp(table_name):
    try:
        a = qdb.get_db_dataframe("SELECT max(ts) FROM %s" %(table_name))
        return pd.to_datetime(a.values[0][0])
    except:
        qdb.print_out("Error in getting maximum timestamp")
        return ''

def update_single_table(noah_gauges):
    noah_id = noah_gauges['dev_id'].values[0]
    gauge_name = noah_gauges['gauge_name'].values[0]
    #check if table "rain_noah_" + "noah_id" exists already
    if qdb.does_table_exist(gauge_name) == False:
        #Create a NOAH table if it doesn't exist yet
        createNOAHTable(gauge_name)
    else:
        qdb.print_out('%s exists' %gauge_name)
    
    #Find the latest timestamp for noahid (which is also the start date)
    latestTS = GetLatestTimestamp(gauge_name)   

    if (latestTS == '') or (latestTS == None):
        #assign a starting date if table is currently empty
        latestTS = '2017-04-01'
    else:
        latestTS = latestTS.strftime("%Y-%m-%d %H:%M:%S")
    
    qdb.print_out("    Start timestamp: %s" % latestTS)
    
    #Generate end time    
    endTS = (pd.to_datetime(latestTS) + timedelta(1)).strftime("%Y-%m-%d")
    qdb.print_out("    End timestamp: %s" %endTS)
    
    #Download data for noahid
    update_table_data(noah_id, gauge_name, latestTS, endTS, noah_gauges)

def main():
    start_time = datetime.now()
    qdb.print_out(start_time)
    
    #get the list of rainfall NOAH rain gauge IDs
    gauges = rain.rainfall_gauges()
    gauges = gauges[gauges.gauge_name.str.contains('noah')].drop_duplicates('gauge_name')
    gauges['dev_id'] = ','.join(gauges.gauge_name).replace('rain_noah_', '').split(',')
    noah_gauges = gauges.groupby('gauge_name')    
    noah_gauges.apply(update_single_table)
    
    qdb.print_out('runtime = %s' %(datetime.now() - start_time))
    
######################################

if __name__ == "__main__": 
    main()