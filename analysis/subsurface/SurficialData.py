##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ion()

import os
import pandas as pd
import numpy as np
from datetime import date, time, datetime, timedelta
from scipy.stats import spearmanr

import configfileio2 as cfg
config = cfg.config()

import rtwindow as rtw
import querySenslopeDb as q
import genproc as g
import platform

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

path = os.path.dirname(os.path.realpath(__file__))

###HARD CODED
surficial_data_path = path + '/SurficialDataFiles/'

def CreateSurficialTable(table_name,verbose=False):
    #### Create Surficial Database table with name table_name

    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()

    cur.execute("USE {}".format(config.dbio.namedb))
    
    if table_name == 'markers': #tapos na
        cur.execute("CREATE TABLE IF NOT EXISTS {}(marker_id smallint(6) AUTO_INCREMENT, description varchar (50), latitude decimal (9, 6), longitude decimal (9, 6), in_use boolean, site_id tinyint unsigned, PRIMARY KEY (marker_id), FOREIGN KEY (site_id) REFERENCES sites(site_id))".format(table_name))
        if verbose:
            print ' markers table CREATED / ALREADY EXISTS '
        db.close()
    elif table_name == 'marker_observations': #tapos na
        cur.execute("CREATE TABLE IF NOT EXISTS {}(observation_id int AUTO_INCREMENT, ts timestamp, meas_type varchar (10), observer_name varchar (50), data_source varchar (4), reliability boolean, weather varchar(10), PRIMARY KEY (observation_id))".format(table_name))
        if verbose:   
            print ' marker_observations table CREATED / ALREADY EXISTS '        
        db.close()
    elif table_name == 'marker_data': #tapos na
        cur.execute("CREATE TABLE IF NOT EXISTS {}(data_id int AUTO_INCREMENT, measurement float, marker_id smallint(6),  observation_id int, PRIMARY KEY (data_id), FOREIGN KEY (marker_id) REFERENCES markers (marker_id), FOREIGN KEY (observation_id) REFERENCES marker_observations (observation_id))".format(table_name))
        if verbose:   
            print ' marker_data table CREATED / ALREADY EXISTS ' 
        db.close()
    elif table_name == 'marker_history': #tapos na
        cur.execute("CREATE TABLE IF NOT EXISTS {}(history_id int AUTO_INCREMENT, marker_id smallint(6), ts timestamp, event varchar(20), PRIMARY KEY (history_id), FOREIGN KEY (marker_id) REFERENCES markers (marker_id))".format(table_name))
        if verbose:   
            print ' marker_history table CREATED / ALREADY EXISTS ' 
        db.close()
    elif table_name == 'marker_names':
        cur.execute("CREATE TABLE IF NOT EXISTS {}(name_id int(11) AUTO_INCREMENT, history_id int, marker_name varchar (20), PRIMARY KEY (name_id), FOREIGN KEY (history_id) REFERENCES marker_history (history_id))".format(table_name))
        if verbose:   
            print ' marker_names table CREATED / ALREADY EXISTS ' 
        db.close()


def CreateAllSurficialTables():
    #### Creates all Surficial Database Tables in correct order
    for table_name in ['markers','marker_observations','marker_data','marker_history','marker_names']:
        CreateSurficialTable(table_name)

def GetSiteID(code):
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))
    
    cur.execute('SELECT site_id FROM sites WHERE site_code = "{}"'.format(code))
    
    try:
        site_id = cur.fetchone()[0]
        db.close()
        return site_id
    except:
        print "ERROR in sites database"
        db.close()

def GetMarkerID(site_id,marker_name = None,lat_long = None): 
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))    

    if lat_long == None and marker_name == None:
        print "ERROR specify lat long or marker name"
    else:
        try:
            cur.execute('SELECT markers.marker_id FROM markers INNER JOIN marker_history ON markers.site_id = {} AND marker_history.marker_id = markers.marker_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id AND marker_names.marker_name = "{}"'.format(site_id,marker_name))
            marker_id = cur.fetchone()[0]
            db.close()
            return marker_id
        except:
            cur.execute('SELECT markers.marker_id FROM markers WHERE markers.site_id = {} AND markers.latitude = {} AND markers.longitude = {}'.format(site_id,lat_long[0],lat_long[1]))
            marker_id = cur.fetchone()[0]
            db.close()
            return marker_id

def ReliabilityToBOOL(reliability):
    if reliability == 'Y':
        return 1
    elif reliability == 'N':
        return 0
    else:
        return 1

def InsertRenamedMarkers(renamed_markers):
    code = renamed_markers['code'].values[0]
    site_id = GetSiteID(code)
    marker_name = renamed_markers['marker_name'].values[0]
    previous_name = renamed_markers['previous_name'].values[0]
    lat_long = (renamed_markers['latitude'].values[0],renamed_markers['longitude'].values[0])
    
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))       
    
    try:
        cur.execute('SELECT markers.marker_id FROM markers INNER JOIN marker_history ON markers.site_id = {} AND marker_history.marker_id = markers.marker_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id AND marker_names.marker_name = "{}"'.format(site_id,marker_name))

        marker_id = cur.fetchone()[0]
        print "Marker {} for site {} exists in database".format(marker_name,code.upper())
    except:
        marker_id = GetMarkerID(site_id,previous_name,lat_long)
 
        
        cur.execute('INSERT INTO marker_history(marker_id,ts,event) VALUES ({},"{}","{}")'.format(marker_id,pd.to_datetime(renamed_markers.ts.values[0]),renamed_markers.operation.values[0]))
        db.commit()
        cur.execute('INSERT INTO marker_names(history_id,marker_name) VALUES(@@IDENTITY,"{}")'.format(marker_name))
        db.commit()
        db.close()


    
def InsertNewMarkers(new_markers):
    #### Get list of all markers used or has been used in a specific site  
    code = new_markers['code'].values[0]
    marker_name = new_markers['marker_name'].values[0]
    description = new_markers.description.values[0]
    latitude = new_markers.latitude.values[0]
    longitude = new_markers.longitude.values[0]
    #### Check for null values
    if not(description == description): #pd.isnull(description)
        description = 'No description'
    if not(latitude == latitude):
        latitude = 'null'
    if not(longitude == longitude):
        longitude = 'null'
    
    site_id = GetSiteID(code)

    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))    
    
    cur.execute('SELECT marker_names.marker_name FROM markers INNER JOIN marker_history ON markers.site_id = {} AND marker_history.marker_id = markers.marker_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id WHERE marker_name = "{}"'.format(site_id,marker_name))
    
    try:
        marker_name = cur.fetchone()[0]
        print "Marker {} for site {} exists in database".format(marker_name,code.upper())
    except:
        cur.execute('INSERT INTO markers (description, latitude, longitude, in_use, site_id) VALUES ("{}",{},{},1,{})'.format(description,latitude,longitude,site_id))
        db.commit()
        cur.execute('INSERT INTO marker_history(marker_id,ts,event) VALUES (@@IDENTITY,"{}","{}")'.format(pd.to_datetime(new_markers.ts.values[0]),new_markers.operation.values[0]))
        db.commit()
        cur.execute('INSERT INTO marker_names(history_id,marker_name) VALUES(@@IDENTITY,"{}")'.format(marker_name))
        db.commit()        
        db.close()

def GetMarkerIDArray(df):
    try:    
        marker_id = GetMarkerID(GetSiteID(df.code),df.marker_name)
        return marker_id
    except:
        print "Marker {} for site {} is not found in database".format(df.code,df.marker_name)
    
def FindReplaceNull(x):
    if not(x==x):
        return 'null'
    else:
        return x

def InsertOtherHistories(other_histories):
    marker_id = other_histories.marker_id.values[0]
    ts = pd.to_datetime(other_histories.ts.values[0])
    operation = other_histories.operation.values[0]
    
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))    
    
    #### Check for duplicates
    cur.execute('SELECT marker_history.history_id FROM marker_history WHERE ts = "{}" AND marker_id = {} AND event = "{}"'.format(ts,marker_id,operation))
    
    try:
        history_id = cur.fetchone()[0]
        print "Entry {} for {} of Marker {} in {} already exists in the database with id {}.".format(ts.strftime('%Y-%m-%d %H:%M:%S'),operation,other_histories.marker_name.values[0],other_histories.code.values[0],history_id)
    except:
        cur.execute('INSERT INTO marker_history(marker_id,ts,event) VALUES ({},"{}","{}")'.format(marker_id,ts,operation))
        db.commit()
        
        #### Kill deactivated markers
        if operation == 'deactivate':
            cur.execute('UPDATE markers SET in_use = 0 WHERE marker_id = {}'.format(marker_id))
            db.commit()

        db.close()


def UpdateSurficialMarkers(csv_filename):
    #### Updates Surficial Markers and Marker History database using csv file

    file_path = surficial_data_path + csv_filename + '.csv'
    marker_history_df = pd.read_csv(file_path)
    
    #### Process all new markers
    new_markers = marker_history_df[marker_history_df.operation == 'add']
    new_markers_group = new_markers.groupby(['code','marker_name'])
    new_markers_group.apply(InsertNewMarkers)
    
    #### Process all renamed markers
    renamed_markers = marker_history_df[marker_history_df.operation == 'rename']
    renamed_markers_group = renamed_markers.groupby(['code','marker_name'])
    renamed_markers_group.apply(InsertRenamedMarkers)
    
    #### Process all other histories
    other_histories = marker_history_df[np.logical_and(~(marker_history_df.operation == 'add'),~(marker_history_df.operation == 'rename'))]
    other_histories.loc[:,'marker_id'] = pd.Series(other_histories.apply(GetMarkerIDArray, axis =1), index = other_histories.index)
    other_histories_group = other_histories.groupby(['marker_id','ts','operation'])
    other_histories_group.apply(InsertOtherHistories)
        
    #### Notify the end of updating
    print "Marker history data from {}.csv has been successfully integrated".format(csv_filename)

def UpdateSurficialObservations(surficial_observations):
#    file_path = surficial_data_path + surficial_observations + '.csv'
#    surficial_observations = pd.read_csv(file_path)
#
#    surficial_observations.columns = ['site_code','marker_name','ts','meas_type','observer_name','weather','data_source','meas','reliability']
#
#    surficial_observations = surficial_observations.applymap(FindReplaceNull)

    ts = pd.to_datetime(surficial_observations.ts.values[0])
    meas_type = surficial_observations.meas_type.values[0]
    observer_name = surficial_observations.observer_name.values[0]
    reliability = surficial_observations.reliability.values[0]
    weather = surficial_observations.weather.values[0]
    data_source = surficial_observations.data_source.values[0]
    
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))    
#    
    cur.execute('SELECT marker_observations.observation_id FROM marker_observations WHERE ts = "{}" AND meas_type = "{}" AND observer_name = "{}" AND reliability = {} AND weather = "{}" AND data_source = "{}"'.format(ts,meas_type,observer_name,reliability,weather,data_source))    
#    
    try:
        so_id =cur.fetchone()[0]
        print "Duplicate entry check so_id = {}".format(so_id)
    except:
        cur.execute('INSERT INTO marker_observations(ts,meas_type,observer_name,reliability,weather,data_source) VALUES ("{}","{}","{}",{},"{}","{}")'.format(ts,meas_type,observer_name,reliability,weather,data_source))
        db.commit()
        surficial_observations.loc[:,'marker_id'] = surficial_observations.apply(GetMarkerIDArray,axis = 1)
        
        cur.execute('SELECT marker_observations.observation_id FROM marker_observations WHERE ts = "{}" AND meas_type = "{}" AND observer_name = "{}" AND reliability = {} AND weather = "{}" AND data_source = "{}"'.format(ts,meas_type,observer_name,reliability,weather,data_source))    
        so_id =cur.fetchone()[0]
        surficial_observations['so_id'] = so_id
        values = [tuple(x) for x in surficial_observations[['marker_id','meas','so_id']].values]
        print values
        cur.executemany('INSERT INTO marker_data(marker_id,measurement,observation_id) VALUES(%s,%s,%s)',values)        
        db.commit()
        db.close()
        
def UpdateSurficialDatabase(csv_filename):
    file_path = surficial_data_path + csv_filename + '.csv'
    surficial_data_df = pd.read_csv(file_path)
    surficial_data_df = surficial_data_df.applymap(FindReplaceNull)
    surficial_data_df.loc[:,'reliability'] = pd.Series(surficial_data_df['reliability'].apply(ReliabilityToBOOL),index = surficial_data_df.index)
    surficial_observations = surficial_data_df.groupby(['code','ts','meas_type','observer_name','reliability','weather','data_source'])
    surficial_observations.apply(UpdateSurficialObservations)






