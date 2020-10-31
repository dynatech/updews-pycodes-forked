from datetime import datetime, timedelta
import os
import pandas as pd
import requests
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import volatile.memory as mem


def download_rainfall_noah(noah_id, fdate, tdate):   
    """Downloads rainfall data of noah_id from fdate to tdate.
    
    Args:
        noah_id (int): Device id of noah data.
        fdate (timestamp): Timestamp start of data to be downloaded.
        tdate (timestamp): Timestamp end of data to be downloaded.

    Returns:
        dataframe: Rainfall data of noah_id from fdate to tdate if with data
                    else empty dataframe.
    
    """

    #Reduce latest_ts by 1 day as a work around for GMT to local conversion
    offset_date = (pd.to_datetime(fdate) - timedelta(1)).strftime("%Y-%m-%d")
    
    sc = mem.server_config()
    url = (sc['rainfall']['noah_data'] + '/%s/from/%s/to/%s') %(noah_id, offset_date, tdate)
    try:
        req = requests.get(url, auth=(sc['rainfall']['noah_user'],
                                      sc['rainfall']['noah_password']))
    except:
        qdb.print_out("Can't get request. Please check internet connection")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(req.json()["data"])
    except:
        qdb.print_out("error: %s" % noah_id)
        return pd.DataFrame()

    try:
        #rename dateTimeRead into ts and rain_value into rain
        df = df.rename(columns = {'rain_value': 'rain', 'dateTimeRead': 'ts'})
        
        df = df.drop_duplicates('ts')
        df['ts'] = df['ts'].apply(lambda x: pd.to_datetime(str(x)[0:19]))
        df['rain'] = df['rain'].apply(lambda x: float(x))
        df = df.sort_values('ts')
        
        #remove the entries that are less than fdate
        df = df[df.ts > fdate]            
        
        return df[['ts', 'rain']]
        
    except:
        return pd.DataFrame()
        
#insert the newly downloaded data to the database
def update_table_data(noah_id, gauge_name, fdate, tdate, noah_gauges):
    """Updates data of table gauge_name from fdate to tdate.
    
    Args:
        noah_id (int): Device id of noah data.
        gauge_name (str): Name of table containing rainfall data of noah_id.
        fdate (timestamp): Timestamp start of data to be downloaded.
        tdate (timestamp): Timestamp end of data to be downloaded.
        noah_gauges (dataframe): Rain gauge properties- id, name, data source.

    """


    noah_data = download_rainfall_noah(noah_id, fdate, tdate)
    cur_ts = datetime.now()
    
    if noah_data.empty: 
        qdb.print_out("    no data...")
        
        #Insert an entry with values: [timestamp, -1] as a marker
        #-1 values should not be included in computation of cml rainfall
        if pd.to_datetime(tdate) <= cur_ts:
            place_holder_data = pd.DataFrame({"ts": [tdate], "rain": [-1.0]})
            qdb.write_rain_data(gauge_name, place_holder_data)
            
    else:        
        #Insert the new data on the noahid table
        qdb.write_rain_data(gauge_name, noah_data)
        
    #The table is already up to date
    if pd.to_datetime(tdate) > cur_ts:
        return         
    else:
        #call this function again until the maximum recent timestamp is hit        
        update_single_table(noah_gauges)
    
def update_single_table(noah_gauges):
    """Updates data of table gauge_name.
    
    Args:
        noah_gauges (dataframe): Rain gauge properties- id, name, data source.
    
    """

    noah_id = noah_gauges['gauge_name'].values[0]
    gauge_name = 'rain_noah_%s' %noah_id
    
    #check if table gauge_name exists
    if qdb.does_table_exist(gauge_name) == False:
        #Create a NOAH table if it doesn't exist yet
        qdb.print_out("Creating NOAH table '%s'" %gauge_name)
        qdb.create_NOAH_table(gauge_name)
    else:
        qdb.print_out('%s exists' %gauge_name)
    
    #Find the latest timestamp for noah_id (which is also the start date)
    latest_ts = qdb.get_latest_ts(gauge_name)   

    if (latest_ts == '') or (latest_ts == None) or latest_ts < datetime.now() - timedelta(3):
        #assign a starting date if table is currently empty
        latest_ts = (datetime.now() - timedelta(3)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        latest_ts = latest_ts.strftime("%Y-%m-%d %H:%M:%S")
    
    qdb.print_out("    Start timestamp: %s" % latest_ts)
    
    #Generate end time    
    end_ts = (pd.to_datetime(latest_ts) + timedelta(1)).strftime("%Y-%m-%d")
    qdb.print_out("    End timestamp: %s" %end_ts)
    
    #Download data for noah_id
    update_table_data(noah_id, gauge_name, latest_ts, end_ts, noah_gauges)

def main():
    """Updates data of NOAH rain gauges.
        
    """
    
    start_time = datetime.now()
    qdb.print_out(start_time)
    
    #get the list of rainfall NOAH rain gauge IDs
    gauges = mem.get('df_rain_props')
        
    gauges = gauges[gauges.data_source == 'noah'].drop_duplicates('rain_id')
    noah_gauges = gauges.groupby('rain_id')    
    noah_gauges.apply(update_single_table)
    
    qdb.print_out('runtime = %s' %(datetime.now() - start_time))
    
######################################

if __name__ == "__main__": 
    main()
