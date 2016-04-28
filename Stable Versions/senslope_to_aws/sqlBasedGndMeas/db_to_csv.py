import os,time,serial,re
import MySQLdb
import datetime
import ConfigParser
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as td
import platform

#---------------------------------------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------------------------------------

def SenslopeDBConnect():
    while True:
        try:
            db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb, db = Namedb)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
            print '.',

def InitLocalDB():
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    db.close()
    
def checkTableExistence(table):
    db, cur = SenslopeDBConnect()
    query = "SHOW TABLES LIKE '%s'" % table
    #print query
    ret = 0
    try:
        cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "checkTableExistence: Error"
        ret = 0
    finally:
        db.close()
        return ret    
    
def checkSiteOnMarkerTable(siteName):
    db, cur = SenslopeDBConnect()
    query = "SELECT * FROM upload_marker_gndmeas WHERE name = '%s'" % siteName
    #print query
    ret = 0
    try:
        cur.execute(query)
        ret = cur.fetchone()
    except TypeError:
        print "checkSiteOnMarkerTable: Error"
        ret = 0
    finally:
        db.close()
        return ret 

def updateSiteOnMarkerTable(siteName, lastUpdate):
    db, cur = SenslopeDBConnect()
    query = "INSERT INTO upload_marker_gndmeas (name,lastupdate) "
    query = query + "VALUES ('%s','%s') " % (siteName, lastUpdate)
    query = query + "ON DUPLICATE KEY "
    query = query + "UPDATE lastupdate = '%s'" % (lastUpdate)

    try:
        cur.execute(query)
        db.commit()
    except TypeError:
        print "updateSiteOnMarkerTable: Error"

    finally:
        db.close()
    
def createUploadMarkerTable(tableName):
    #Create the upload marker table if it doesn't exist yet
    doesTableExist = checkTableExistence(tableName)          

    if doesTableExist == 0:
        #create table before adding data
        print "creating upload marker table tableName!"     
    
        db, cur = SenslopeDBConnect()
        
        query = "CREATE TABLE `senslopedb`.`"+tableName+"` ("
        query = query + "`name` VARCHAR(8) NOT NULL, "
        query = query + "`lastupdate` DATETIME NOT NULL, "
        query = query + "PRIMARY KEY (`name`));"
        
        cur.execute(query)
        db.close()
        
    return doesTableExist


#Get the last timetamp on target sql file to be uploaded
#Returns a timestamp for valid entries
def getTimestampEnd(tableName, start, version = 'senslope'):
    db, cur = SenslopeDBConnect()
    
    multiplier = 1
    tryAgain = True  
    
    while tryAgain:
        dateReso = 10 * multiplier
        end = (pd.to_datetime(start) + td(dateReso)).strftime("%Y-%m-%d %H:%M:%S")
        
        query = "SELECT COUNT(*) AS count FROM gndmeas "
        query = query + "WHERE timestamp > '%s' AND timestamp < '%s' " % (start, end)
        query = query + "AND site_id = '%s' " % (tableName)    
        query = query + "ORDER BY timestamp DESC"
        
        cur.execute(query)
        count = cur.fetchall()[0][0]
        
        multiplier += 1        
        
        #don't try again when count is greater than zero or end timestamp is
        #   greater than the current timestamp
        curTS = time.strftime("%Y-%m-%d %H:%M:%S")
        if (count > 0) or (end >= curTS):
            tryAgain = False
            
            #get max timestamp
            query = "SELECT MAX(timestamp) as ts from gndmeas "
            query = query + "WHERE timestamp > '%s' AND timestamp < '%s' " % (start, end)
            query = query + "AND site_id = '%s' " % (tableName)            
            
            cur.execute(query)
            maxTS = cur.fetchall()[0][0]
            
            return maxTS
    
    pass
    return None

    
""" Global variables"""
cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')
SleepPeriod = cfg.getint('Misc','SleepPeriod')

operatingSystem = platform.system()
print operatingSystem

if operatingSystem == 'Windows':
    outputPath = cfg.get('Folders', 'windowsOutput')
elif operatingSystem == 'Linux':
    tempPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
    outputFolder = cfg.get('Folders', 'linuxOutput')
    outputPath = tempPath + "/" + outputFolder
    
    #Create the 'linuxOutput' folder if it doesn't exist yet
    if not os.path.exists(outputPath):
        os.makedirs(outputPath)
    
print "output path = " + outputPath

def extractDBToSQL(table, doesTableExist = 1, version = 3):    
    TSstart = 0
    isFirstUpload = False
    
    #initialize config file name
    ts_site = 'ts_' + table
    print ts_site

    #Check if the current site column exists in "upload_marker_gndmeas"
    lastUpdate = checkSiteOnMarkerTable(table)

    if lastUpdate == None:
        #Create default value of 2010-10-01 00:00:00
        #   if not found on config file
        TSstart = '2010-10-01 00:00:00'

        #This boolean will trigger the creation of sql that is
        #   capable of creating a table            
        isFirstUpload = True
        
        #Insert the timestamp as lastupdate value on "upload_marker_gndmeas"
        updateSiteOnMarkerTable(table, TSstart)
        
        pass
    else:
        #get last timestamp from DB return value
        TSstart = lastUpdate[1].strftime("%Y-%m-%d %H:%M:%S")
        #print '>> lastUpdate has a value %s' % (TSstart)  
    
    print '>> Extracting %s data from database.. TS Start: %s' % (table, TSstart)  

    TSend = getTimestampEnd(table, TSstart, version)
    
    #Return if there is no new data
    if TSend == None:
        print '>> Current lastupdate is latest data or site has no new data'
        return
    else:
        TSend = TSend.strftime("%Y-%m-%d %H:%M:%S")

    tsStartParsed = re.sub('[.!,;:]', '', TSstart)
    tsStartParsed = re.sub(' ', '_', tsStartParsed)
    
#    fullPath = 'D:\\dewslandslide\\gndmeas_' + table + '_' + tsStartParsed + '.sql'
    fullPath = outputPath + 'gndmeas_' + table + '_' + tsStartParsed + '.sql'
    winCmd = None

    #SQL creation is different for a site's first time upload of data
    if doesTableExist:
        #WILL NOT Overwrite. Good for just updating your DB tables
        winCmd = 'mysqldump -t -u %s -p%s senslopedb gndmeas' % (Userdb, Passdb)        
    else:
        #Overwrites table if it exists on your database already
        winCmd = 'mysqldump -u %s -p%s senslopedb gndmeas' % (Userdb, Passdb)
        
    winCmd = winCmd + ' --where="timestamp > \'%s\' and timestamp <= \'%s\' and site_id = \'%s\'" > ' % (TSstart, TSend, table) 
    winCmd = winCmd + fullPath

    print 'winCmd = ' + winCmd + '\n'
    
    try:
        os.system(winCmd)
        
        #Update the timestamp as lastupdate value on "upload_marker_gndmeas"
        updateSiteOnMarkerTable(table, TSend)
    except:
        print ">> Error on executing on command line"

    time.sleep(3)
    print 'done'


def extract_db():
    doesTableExist = createUploadMarkerTable("upload_marker_gndmeas")
    
    try:
        db, cur = SenslopeDBConnect()
        print '>> Connected to database'

        query = 'SELECT DISTINCT LEFT(name, 3) as site_code FROM site_column ORDER BY site_code ASC'
        try:
            cur.execute(query)
        except:
            print '>> Error parsing database'
        
        data = cur.fetchall()

        for table in data:
            extractDBToSQL(table[0])
            
            #set table existence to one after first run (Ugly Quick Fix)
            #doesTableExist = 1

    except IndexError:
        print '>> Error in writing extracting database data to files..'

