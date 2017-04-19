from datetime import timedelta, time
import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine

import querydb as q

def create_rainfall_alerts():
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `rainfall_alerts` ("
    query += "  `ra_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL DEFAULT '2010-01-01 00:00:00',"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `rain_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `rain_alert` CHAR(3) NOT NULL,"
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

def create_site_alerts():
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `site_alerts` ("
    query += "  `sa_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL DEFAULT '2010-01-01 00:00:00',"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `alert_source` VARCHAR(10) NOT NULL,"
    query += "  `alert_level` VARCHAR(10) NOT NULL,"
    query += "  `ts_updated` TIMESTAMP NOT NULL DEFAULT '2010-01-01 00:00:00',"
    query += "  PRIMARY KEY (`sa_id`),"
    query += "  INDEX `fk_site_alerts_sites1_idx` (`site_id` ASC),"
    query += "  UNIQUE INDEX `uq_site_alerts` (`ts` ASC, `site_id` ASC, `alert_source` ASC),"
    query += "  CONSTRAINT `fk_rainfall_alerts_sites1`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE CASCADE"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()

def toDB_site_alerts(df, end):
    
    query = "SELECT * FROM site_alerts WHERE site_id = '%s' AND alert_source = 'rain' AND ts_updated <= '%s' ORDER BY ts DESC LIMIT 1" %(df['site_id'].values[0], end)
    
    df2 = q.GetDBDataFrame(query)
    
    if len(df2) == 0 or df2['alert_level'].values[0] != df['alert_level'].values[0]:
        df['ts_updated'] = end
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = 'site_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    elif df2['alert_level'].values[0] == df['alert_level'].values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE site_alerts SET ts_updated = '%s' WHERE site_id = '%s' and alert_source = 'rain' and alert_level = '%s' and ts = '%s'" %(end, df2['site_id'].values[0], df2['alert_level'].values[0], df2['ts'].values[0])
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
    print gauge_name, '\n', rainfall
    try:
        if rainfall.index[-1] <= end-timedelta(1):
            return pd.DataFrame()
        
        #data resampled to 30mins
        if rainfall.index[-1]<end:
            blankdf=pd.DataFrame({'ts': [end], 'rain': [0]})
            blankdf=blankdf.set_index('ts')
            rainfall=rainfall.append(blankdf)
        rainfall=rainfall.resample('30min',how='sum', label='right')
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
    rainfall2=pd.rolling_sum(rainfall,48,min_periods=1)
    rainfall2=np.round(rainfall2,4)
    
    #getting the rolling sum for the last 3 days
    rainfall3=pd.rolling_sum(rainfall,144,min_periods=1)
    rainfall3=np.round(rainfall3,4)

            
    one = float(rainfall2.rain[-1:])
    three = float(rainfall3.rain[-1:])
    
    return one,three
        
def summary_writer(site_id,gauge_name,rain_id,twoyrmax,halfmax,rainfall,end,write_alert):

    ##DESCRIPTION:
    ##inserts data to summary

    ##INPUT:
    ##s; float; index    
    ##r; string; site code
    ##datasource; string; source of data: ASTI1-3, SENSLOPE Rain Gauge
    ##twoyrmax; float; 2-yr max rainfall, threshold for three day cumulative rainfall
    ##halfmax; float; half of 2-yr max rainfall, threshold for one day cumulative rainfall
    ##summary; dataframe; contains site codes with its corresponding one and three days cumulative sum, data source, alert level and advisory
    ##alert; array; alert summary container, r0 sites at alert[0], r1a sites at alert[1], r1b sites at alert[2],  nd sites at alert[3]
    ##alert_df;array of tuples; alert summary container; format: (site,alert)
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall        
    
    one,three = onethree_val_writer(rainfall)

    #threshold is reached
    if one>=halfmax or three>=twoyrmax:
        ralert='r1'
        advisory='Start/Continue monitoring'
    #no data
    elif one==None or math.isnan(one):
        ralert='nd'
        advisory='---'
    #rainfall below threshold
    else:
        ralert='r0'
        advisory='---'

    if (write_alert and end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]) or ralert == 'r1':
        if q.DoesTableExist('rainfall_alerts') == False:
            #Create a site_alerts table if it doesn't exist yet
            create_rainfall_alerts()

        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        if ralert == 'r0':
            if one < halfmax*0.75 and three < twoyrmax*0.75:                
                df = pd.DataFrame({'ts': [end,end], 'site_id': [site_id,site_id], 'rain_id': [rain_id,rain_id], 'rain_alert': ['r0a','r0b'], 'cumulative': [one,three], 'threshold': [round(halfmax,2),round(twoyrmax,2)]})
                try:
                    df.to_sql(name = 'rainfall_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                except:
                    pass
        else:
            if one >= halfmax:                
                df = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'rain_id': [rain_id], 'rain_alert': ['r1a'], 'cumulative': [one], 'threshold': [round(halfmax,2)]})
                try:
                    df.to_sql(name = 'rainfall_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                except:
                    pass
            if three>=twoyrmax:                
                df = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'rain_id': [rain_id], 'rain_alert': ['r1b'], 'cumulative': [three], 'threshold': [round(twoyrmax,2)]})
                try:
                    df.to_sql(name = 'rainfall_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)        
                except:
                    pass

    summary = pd.DataFrame({'site_id': [site_id], '1D cml': [one], 'half of 2yr max': [round(halfmax,2)], '3D cml': [three], '2yr max': [round(twoyrmax,2)], 'DataSource': [gauge_name], 'rain_id': [rain_id], 'alert': [ralert], 'advisory': [advisory]})
    
    return summary

def main(rain_props, end, s):

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
        query = "SELECT * FROM site_alerts where site_id = '%s' and source in ('public') order by ts desc limit 1" %site_id
        df = q.GetDBDataFrame(query)
        currAlert = df['alert'].values[0]
        if currAlert != 'A0':
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
        summary = summary_writer(site_id,gauge_name,rain_id,twoyrmax,halfmax,rainfall,end,write_alert)

    if q.DoesTableExist('site_alerts') == False:
        #Create a site_alerts table if it doesn't exist yet
        create_site_alerts()
        
    site_alerts = summary[['site_id', 'alert']]
    site_alerts['ts'] = str(end)
    site_alerts['alert_source'] = 'rain'
    site_alerts = site_alerts.rename(columns = {'alert': 'alert_level'})
    toDB_site_alerts(site_alerts, end)

    return summary