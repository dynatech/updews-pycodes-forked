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
    query = "SELECT * FROM upload_marker_raingauge WHERE name = '%s'" % siteName
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
    query = "INSERT INTO upload_marker_raingauge (name,lastupdate) "
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

#Create aws table if it doesn't exist yet
def createAwsTable(tableName, awsType = 'senslope'):
    #Create aws table if it doesn't exist yet
    doesTableExist = checkTableExistence(tableName)  
    
    if doesTableExist == 0:
        print ">>> Create table %s..." % (tableName)
        db, cur = SenslopeDBConnect()
        query = "CREATE TABLE `senslopedb`.`"+tableName+"` ("
        
        #createAwsTable if type is rain_senslope
        if (awsType == "senslope") or (awsType == 0):
            query += "`timestamp` DATETIME NOT NULL,"
            query += "`name` VARCHAR(6) NOT NULL,"
            query += "`temp` DOUBLE NULL,"
            query += "`wspd` INT(11) NULL,"
            query += "`wdir` INT(11) NULL,"
            query += "`rain` DOUBLE NULL,"
            query += "`batt` DOUBLE NULL,"
            query += "`csq` INT(11) NULL,"
            query += "PRIMARY KEY (`timestamp`, `name`));  "
            
        #createAwsTable if type is rain_arq
        elif (awsType == "arq") or (awsType == 1):
            query += "`timestamp` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',"
            query += "`name` VARCHAR(6) NOT NULL DEFAULT '',"
            query += "`r15m` DOUBLE NULL,"
            query += "`r24h` DOUBLE NULL,"
            query += "`batv1` DOUBLE NULL,"
            query += "`batv2` DOUBLE NULL,"
            query += "`cur` DOUBLE NULL,"
            query += "`boostv1` DOUBLE NULL,"
            query += "`boostv2` DOUBLE NULL,"
            query += "`charge` INT(11) NULL,"
            query += "`csq` INT(11) NULL,"
            query += "`temp` DOUBLE NULL,"
            query += "`hum` DOUBLE NULL,"
            query += "`flashp` INT(11) NULL,"
            query += "PRIMARY KEY (`timestamp`, `name`));"

        cur.execute(query)
        db.close()


#Get the last timetamp on target sql file to be uploaded
#Returns a timestamp for valid entries
def getTimestampEnd(tableName, start, awsType = 'senslope'):
    db, cur = SenslopeDBConnect()
    
    #Create aws table if it doesn't exist yet
    createAwsTable(tableName, awsType)
    
    multiplier = 1
    tryAgain = True  
    
    while tryAgain:
        dateReso = 10 * multiplier
        end = (pd.to_datetime(start) + td(dateReso)).strftime("%Y-%m-%d %H:%M:%S")
        
        query = "SELECT COUNT(*) AS count from %s " % (tableName)
        query = query + "WHERE timestamp > '%s' AND timestamp < '%s' " % (start, end)
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
            query = "SELECT MAX(timestamp) as ts from %s " % (tableName)
            query = query + "WHERE timestamp > '%s' AND timestamp < '%s' " % (start, end)          
            
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

def extractDBToSQL(table, awsType = 'senslope'):    
    TSstart = 0
    isFirstUpload = False
    
    #initialize config file name
    ts_site = 'ts_' + table
    print ts_site

    #Check if the current site column exists in "upload_marker_raingauge"
    lastUpdate = checkSiteOnMarkerTable(table)

    if lastUpdate == None:
        #Get input from config file if it doesn't exist yet
        cfg = ConfigParser.ConfigParser()
        cfg.read('senslope-server-config.txt')
    
        try:
            #The new time start is the last TimeStampEnd from config file
            TSstart = cfg.get('Misc', ts_site)
        except:
            #Create default value of 2010-10-01 00:00:00
            #   if not found on config file
            TSstart = '2010-10-01 00:00:00'

            #This boolean will trigger the creation of sql that is
            #   capable of creating a table            
            isFirstUpload = True
        
        #Insert the timestamp as lastupdate value on "upload_marker_raingauge"
        updateSiteOnMarkerTable(table, TSstart)
    else:
        #get last timestamp from DB return value
        TSstart = lastUpdate[1].strftime("%Y-%m-%d %H:%M:%S")
        #print '>> lastUpdate has a value %s' % (TSstart)  
    
    print '>> Extracting %s data from database.. TS Start: %s' % (table, TSstart)  

    TSend = getTimestampEnd(table, TSstart, awsType)
    
    #Return if there is no new data
    if TSend == None:
        print '>> Current lastupdate is latest data or site has no new data'
        return
    else:
        TSend = TSend.strftime("%Y-%m-%d %H:%M:%S")

    tsStartParsed = re.sub('[.!,;:]', '', TSstart)
    tsStartParsed = re.sub(' ', '_', tsStartParsed)
    
#    fullPath = 'D:\\dewslandslide\\' + table + '_' + tsStartParsed + '.sql'
    fullPath = outputPath + table + '_' + tsStartParsed + '.sql'
    winCmd = None

    #SQL creation is different for a site's first time upload of data
    if isFirstUpload:
        #Overwrites table if it exists on your database already
        winCmd = 'mysqldump -u %s -p%s senslopedb %s' % (Userdb, Passdb, table)
    else:
        #WILL NOT Overwrite. Good for just updating your DB tables
        winCmd = 'mysqldump -t -u %s -p%s senslopedb %s' % (Userdb, Passdb, table)
        
    winCmd = winCmd + ' --where="timestamp > \'%s\' and timestamp <= \'%s\'" > ' % (TSstart, TSend) 
    winCmd = winCmd + fullPath

    print 'winCmd = ' + winCmd + '\n'
    
    try:
        os.system(winCmd)
        
        #Update the timestamp as lastupdate value on "upload_marker_raingauge"
        updateSiteOnMarkerTable(table, TSend)
    except:
        print ">> Error on executing on command line"

    time.sleep(3)
    print 'done'


def extract_db():
    createUploadMarkerTable("upload_marker_raingauge")
    
    try:
        db, cur = SenslopeDBConnect()
        print '>> Connected to database'

        #get rain_senslope items
        queryAwsOld = 'SELECT DISTINCT rain_senslope FROM site_rain_props WHERE '
        queryAwsOld = queryAwsOld + 'rain_senslope IS NOT NULL'
        
        try:
            cur.execute(queryAwsOld)
        except:
            print '>> Error parsing database'
        
        awsOld = cur.fetchall()

        for table in awsOld:
            extractDBToSQL(table[0], "senslope")
            
        #get rain_arq items
        queryArq = 'SELECT DISTINCT rain_arq FROM site_rain_props WHERE '
        queryArq = queryArq + 'rain_arq IS NOT NULL'
        
        try:
            cur.execute(queryArq)
        except:
            print '>> Error parsing database'
        
        awsArq = cur.fetchall()

        for table in awsArq:
            extractDBToSQL(table[0], "arq")
            
        pass

    except IndexError:
        print '>> Error in writing extracting database data to files..'

