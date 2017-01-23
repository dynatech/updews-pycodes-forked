from datetime import timedelta
import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine

import querySenslopeDb as q

def GetResampledData(r, offsetstart, start, end):
    
    ##INPUT:
    ##r; str; site
    ##start; datetime; start of rainfall data
    ##end; datetime; end of rainfall data
    
    ##OUTPUT:
    ##rainfall; dataframe containing start to end of rainfall data resampled to 30min
    
    #raw data from senslope rain gauge
    rainfall = q.GetRawRainData(r, offsetstart)
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall.loc[rainfall['rain']>=0]
    
    try:
        if rainfall.index[-1] <= end-timedelta(1):
            return pd.DataFrame(data=None)
        
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
        return pd.DataFrame(data=None)
        
def GetUnemptyOtherRGdata(col, offsetstart, start, end):
    
    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    
    ##OUTPUT:
    ##df; dataframe; rainfall from noah rain gauge    
    
    #gets data from nearest noah/senslope rain gauge
    #moves to next nearest until data is updated
    
    for n in range(3):            
        r = col[n]
        
        OtherRGdata = GetResampledData(r, offsetstart, start, end)
        if len(OtherRGdata) != 0:
            latest_ts = pd.to_datetime(OtherRGdata.index.values[-1])
            if end - latest_ts < timedelta(1):
                return OtherRGdata, r
    return pd.DataFrame(data = None), r

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
        
def summary_writer(r,datasource,twoyrmax,halfmax,rainfall):

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

    summary = pd.DataFrame({'site': [r], '1D cml': [one], 'half of 2yr max': [round(halfmax,2)], '3D cml': [three], '2yr max': [round(twoyrmax,2)], 'DataSource': [datasource], 'alert': [ralert], 'advisory': [advisory]})
    
    return summary

def RainfallAlert(siterainprops, end, s):

    ##INPUT:
    ##siterainprops; DataFrameGroupBy; contains rain noah ids of noah rain gauge near the site, one and three-day rainfall threshold
    
    ##OUTPUT:
    ##evaluates rainfall alert
    
    #rainfall properties from siterainprops
    name = siterainprops['name'].values[0]
    twoyrmax = siterainprops['max_rain_2year'].values[0]
    halfmax=twoyrmax/2
    
    rain_arq = siterainprops['rain_arq'].values[0]
    rain_senslope = siterainprops['rain_senslope'].values[0]
    RG1 = siterainprops['RG1'].values[0]
    RG2 = siterainprops['RG2'].values[0]
    RG3 = siterainprops['RG3'].values[0]
    
    start = end - timedelta(s.io.roll_window_length)
    offsetstart = start - timedelta(hours=0.5)
        
    try:
        if rain_arq == None:
            rain_timecheck = pd.DataFrame()
        else:
            #resampled data from senslope rain gauge
            rainfall = GetResampledData(rain_arq, offsetstart, start, end)
            #data not more than a day from end
            rain_timecheck = rainfall[(rainfall.index>=end-timedelta(days=1))]
        
        #if data from rain_arq is not updated, data is gathered from rain_senslope
        if len(rain_timecheck.dropna())<1:
            #from rain_senslope, plots and alerts are processed
            rainfall = GetResampledData(rain_senslope, offsetstart, start, end)
            datasource = rain_senslope
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall)
                    
        else:
            #alerts are processed if senslope rain gauge data is updated
            datasource = rain_arq
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall)

    except:
        try:
            #if no data from senslope rain gauge, data is gathered from nearest senslope rain gauge from other site or noah rain gauge
            col = [RG1, RG2, RG3]
            rainfall, r = GetUnemptyOtherRGdata(col, offsetstart, start, end)
            datasource = "Other Rain Gauge: %s" %r
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall)
        except:
            #if no data for all rain gauge
            rainfall = pd.DataFrame({'ts': [end], 'rain': [np.nan]})
            rainfall = rainfall.set_index('ts')
            datasource="No Alert! No ASTI/SENSLOPE Data"
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall)
            
    return summary

def alert_toDB(df, end):
    
    query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'rain' AND updateTS <= '%s' ORDER BY updateTS DESC LIMIT 1" %(df.site.values[0], end)
    
    df2 = q.GetDBDataFrame(query)
    
    if len(df2) == 0 or df2.alert.values[0] != df.alert.values[0]:
        df['updateTS'] = end
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    elif df2.alert.values[0] == df.alert.values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.site_level_alert SET updateTS='%s' WHERE site = '%s' and source = 'rain' and alert = '%s' and timestamp = '%s'" %(end, df2.site.values[0], df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
        cur.execute(query)
        db.commit()
        db.close()

################################     MAIN     ################################

def main(siterainprops, end, s):

    ### Processes Rainfall Alert ###
    summary = RainfallAlert(siterainprops, end, s)
    
    dbsummary = summary
    dbsummary['timestamp'] = str(end)
    dbsummary['source'] = 'rain'
    dbsummary = dbsummary[['timestamp', 'site', 'source', 'alert']]
    alert_toDB(dbsummary, end)
    
    return summary
