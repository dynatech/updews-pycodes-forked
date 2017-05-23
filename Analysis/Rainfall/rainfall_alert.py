from datetime import timedelta, time
import math
import numpy as np
import os
import pandas as pd
import sys

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

def create_rainfall_alerts():
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `rainfall_alerts` ("
    query += "  `ra_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NULL,"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `rain_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `rain_alert` CHAR(1) NOT NULL,"
    query += "  `cumulative` DECIMAL(5,2) UNSIGNED NULL,"
    query += "  `threshold` DECIMAL(5,2) UNSIGNED NULL,"
    query += "  PRIMARY KEY (`ra_id`),"
    query += "  INDEX `fk_rainfall_alerts_sites1_idx` (`site_id` ASC),"
    query += "  INDEX `fk_rainfall_alerts_rain_gauges1_idx` (`rain_id` ASC),"
    query += "  UNIQUE INDEX `uq_rainfall_alerts` (`ts` ASC, `site_id` ASC, `rain_alert` ASC),"
    query += "  CONSTRAINT `fk_rainfall_alerts_sites1`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE CASCADE"
    query += "    ON UPDATE CASCADE,"
    query += "  CONSTRAINT `fk_rainfall_alerts_rain_gauges1`"
    query += "    FOREIGN KEY (`rain_id`)"
    query += "    REFERENCES `rainfall_gauges` (`rain_id`)"
    query += "    ON DELETE CASCADE"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()

def GetRawRainData(gauge_name, fromTime="", toTime=""):
    
    try:
        
        query = "SELECT ts, rain from %s.%s " % (q.Namedb, gauge_name)
                        
        if not fromTime:
            fromTime = "2010-01-01"
            
        query = query + " where ts > '%s'" % fromTime
        
        if toTime:
            query = query + " and ts < '%s'" % toTime
    
        query = query + " order by ts"
    
        df =  q.GetDBDataFrame(query)
        
        # change ts column to datetime
        df.ts = pd.to_datetime(df.ts)
        
        return df
        
    except UnboundLocalError:
        print 'No ' + gauge_name + ' table in SQL'    

    return

def GetResampledData(gauge_name, offsetstart, start, end):
    
    ##INPUT:
    ##r; str; site
    ##start; datetime; start of rainfall data
    ##end; datetime; end of rainfall data
    
    ##OUTPUT:
    ##rainfall; dataframe containing start to end of rainfall data resampled to 30min
    
    #raw data from senslope rain gauge
    rainfall = GetRawRainData(gauge_name, fromTime=offsetstart, toTime=end)
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall.loc[rainfall['rain']>=0]

    try:
        if rainfall.index[-1] <= end-timedelta(1):
            return pd.DataFrame()
        
        #data resampled to 30mins
        if rainfall.index[-1]<end:
            blankdf=pd.DataFrame({'ts': [end], 'rain': [0]})
            blankdf=blankdf.set_index('ts')
            rainfall=rainfall.append(blankdf)
        rainfall=rainfall.resample('30min').sum()
        rainfall=rainfall[(rainfall.index>=start)]
        rainfall=rainfall[(rainfall.index<=end)]    
        return rainfall
    except:
        return pd.DataFrame()
        
def GetUnemptyRGdata(rain_props, offsetstart, start, end):
    
    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    
    ##OUTPUT:
    ##df; dataframe; rainfall from noah rain gauge    
    
    #gets data from nearest noah/senslope rain gauge
    #moves to next nearest until data is updated
    
    RG_num = len(rain_props['rainfall_gauges'].values[0])
    
    for n in range(RG_num):            
        gauge_name = rain_props['rainfall_gauges'].values[0][n]
        rain_id = rain_props['rain_id'].values[0][n]
        RGdata = GetResampledData(gauge_name, offsetstart, start, end)
        if len(RGdata) != 0:
            latest_ts = pd.to_datetime(RGdata.index.values[-1])
            if latest_ts > end - timedelta(1):
                return RGdata, gauge_name, rain_id
    return pd.DataFrame()

def onethree_val_writer(rainfall):

    ##INPUT:
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall

    ##OUTPUT:
    ##one, three; float; cumulative sum for one day and three days

    #getting the rolling sum for the last24 hours
    rainfall2 = rainfall.rolling(min_periods=1,window=48,center=False).sum()
    rainfall2 = np.round(rainfall2,4)
    
    #getting the rolling sum for the last 3 days
    rainfall3 = rainfall.rolling(min_periods=1,window=144,center=False).sum()
    rainfall3 = np.round(rainfall3,4)

            
    one = float(rainfall2.rain[-1:])
    three = float(rainfall3.rain[-1:])
    
    return one,three
        
def summary_writer(site_id,gauge_name,rain_id,twoyrmax,halfmax,rainfall,end,write_alert):

    ##DESCRIPTION:
    ##inserts data to summary

    ##INPUT:
    ##twoyrmax; float; 2-yr max rainfall, threshold for three day cumulative rainfall
    ##halfmax; float; half of 2-yr max rainfall, threshold for one day cumulative rainfall
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall        
    
    one,three = onethree_val_writer(rainfall)

    #threshold is reached
    if one>=halfmax or three>=twoyrmax:
        ralert=1
        advisory='Start/Continue monitoring'
    #no data
    elif one==None or math.isnan(one):
        ralert=-1
        advisory='---'
    #rainfall below threshold
    else:
        ralert=0
        advisory='---'

    if (write_alert and end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]) or ralert == 1:
        if q.DoesTableExist('rainfall_alerts') == False:
            #Create a site_alerts table if it doesn't exist yet
            create_rainfall_alerts()

        if ralert == 0:
            if one < halfmax*0.75 and three < twoyrmax*0.75:
                query = "SELECT EXISTS(SELECT * FROM rainfall_alerts"
                query += " WHERE ts = '%s' AND site_id = %s AND rain_alert = '0'" %(end, site_id)
                if q.GetDBDataFrame(query).values[0][0] == 0:
                    df = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'rain_id': [rain_id], 'rain_alert': [0], 'cumulative': [np.nan], 'threshold': [np.nan]})
                    q.PushDBDataFrame(df, 'rainfall_alerts', index = False)

        else:
            if one >= halfmax:
                query = "SELECT EXISTS(SELECT * FROM rainfall_alerts"
                query += " WHERE ts = '%s' AND site_id = %s AND rain_alert = 'a'" %(end, site_id)
                if q.GetDBDataFrame(query).values[0][0] == 0:
                    df = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'rain_id': [rain_id], 'rain_alert': ['a'], 'cumulative': [one], 'threshold': [round(halfmax,2)]})
                    q.PushDBDataFrame(df, 'rainfall_alerts', index = False)
            if three>=twoyrmax:
                query = "SELECT EXISTS(SELECT * FROM rainfall_alerts"
                query += " WHERE ts = '%s' AND site_id = %s AND rain_alert = 'b'" %(end, site_id)
                if q.GetDBDataFrame(query).values[0][0] == 0:
                    df = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'rain_id': [rain_id], 'rain_alert': ['b'], 'cumulative': [three], 'threshold': [round(twoyrmax,2)]})
                    q.PushDBDataFrame(df, 'rainfall_alerts', index = False)

    summary = pd.DataFrame({'site_id': [site_id], '1D cml': [one], 'half of 2yr max': [round(halfmax,2)], '3D cml': [three], '2yr max': [round(twoyrmax,2)], 'DataSource': [gauge_name], 'rain_id': [rain_id], 'alert': [ralert], 'advisory': [advisory]})
    
    return summary

def main(rain_props, end, s, trigger_symbol):

    ##INPUT:
    ##rainprops; DataFrameGroupBy; contains rain noah ids of noah rain gauge near the site, one and three-day rainfall threshold
    
    ##OUTPUT:
    ##evaluates rainfall alert
    
    #rainfall properties
    site_id = rain_props['site_id'].values[0]
    twoyrmax = rain_props['threshold_value'].values[0]
    halfmax=twoyrmax/2
    
    start = end - timedelta(s.io.roll_window_length)
    offsetstart = start - timedelta(hours=0.5)

    try:
        query = "SELECT EXISTS (SELECT * FROM public_alerts as a left join" 
        query += " public_alert_symbols as s on a.pub_sym_id = s.pub_sym_id"
        query += " where site_id = %s and alert_level > 0" %site_id
        query += " and ts <= '%s' and ts_updated >= '%s')" %(end, end)
        if q.GetDBDataFrame(query).values[0][0] == 1:
            write_alert = True
        else:
            write_alert = False
    except:
        write_alert = False

    try:
        #data is gathered from nearest rain gauge
        rainfall, gauge_name, rain_id = GetUnemptyRGdata(rain_props, offsetstart, start, end)
        summary = summary_writer(site_id,gauge_name,rain_id,twoyrmax,halfmax,rainfall,end,write_alert)
    except:
        #if no data for all rain gauge
        rainfall = pd.DataFrame({'ts': [end], 'rain': [np.nan]})
        rainfall = rainfall.set_index('ts')
        gauge_name="No Alert! No ASTI/SENSLOPE Data"
        rain_id="No Alert! No ASTI/SENSLOPE Data"
        summary = summary_writer(site_id,gauge_name,rain_id,twoyrmax,halfmax,rainfall,end,write_alert)

    operational_trigger = summary[['site_id', 'alert']]
    operational_trigger['alert'] = operational_trigger['alert'].map({-1:trigger_symbol[trigger_symbol.alert_level == -1]['trigger_sym_id'].values[0], 0:trigger_symbol[trigger_symbol.alert_level == 0]['trigger_sym_id'].values[0], 1:trigger_symbol[trigger_symbol.alert_level == 1]['trigger_sym_id'].values[0]})
    operational_trigger['ts'] = str(end)
    operational_trigger['ts_updated'] = str(end)
    operational_trigger = operational_trigger.rename(columns = {'alert': 'trigger_sym_id'})
    q.alert_toDB(operational_trigger, 'operational_triggers')

    return summary