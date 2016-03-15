import os,time,serial,re
import MySQLdb
import datetime
import ConfigParser
import pandas as pd
import numpy as np
from datetime import datetime as dt

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
    query = "SELECT * FROM upload_marker_accel WHERE name = '%s'" % siteName
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
    #query = "SELECT * FROM upload_marker_accel WHERE name = '%s'" % siteName
    query = "INSERT INTO upload_marker_accel (name,lastupdate) "
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
  
    
""" Global variables"""
cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')
SleepPeriod = cfg.getint('Misc','SleepPeriod')


def extractDBToSQL(table):    
    TSstart = 0

    #Check if the current site column exists in "upload_marker_accel"
    lastUpdate = checkSiteOnMarkerTable(table)

    if lastUpdate == None:
        #Get input from config file if it doesn't exist yet
        cfg = ConfigParser.ConfigParser()
        cfg.read('senslope-server-config.txt')
    
        try:
            ts_site = 'ts_' + table
            print '>> ts_site = ' + ts_site

            #The new time start is the last TimeStampEnd from config file
            TSstart = cfg.get('Misc', ts_site)
        except:
            #Create default value of 2010-10-01 00:00:00
            #   if not found on config file
            TSstart = '2010-10-01 00:00:00'
        
        #TODO: insert the timestamp as lastupdate value on "upload_marker_accel"
        updateSiteOnMarkerTable(table, TSstart)
        
        pass
    else:
        #get last timestamp from DB return value
        TSstart = lastUpdate[1].strftime("%Y-%m-%d %H:%M:%S")
        print '>> lastUpdate has a value %s' % (TSstart)
        pass    
    
    print '>> Extracting ' + table + ' purged data from database ..\n'  

    print 'TS Start = ' + TSstart + '\n'

    tsStartParsed = re.sub('[.!,;:]', '', TSstart)
    tsStartParsed = re.sub(' ', '_', tsStartParsed)
    fileName = 'D:\\dewslandslide\\TESTF\\' + table + '_' + tsStartParsed + '.sql'

    print 'filename parsed = ' + fileName + '\n'

    winCmd = 'mysqldump -t -u ' + Userdb + ' -p' + Passdb + ' senslopedb ' + table
    winCmd = winCmd + ' --where="timestamp > \'' + TSstart + '\'" > ' + fileName;

    print 'winCmd = ' + winCmd + '\n'

    db, cur = SenslopeDBConnect()
    query_tstamp = 'select max(timestamp) from (SELECT timestamp FROM ' + table
    query_tstamp = query_tstamp + ' where xvalue > 0 and zvalue > -500 '
    query_tstamp = query_tstamp + 'and id > 0 and id < 41 and timestamp > "'
    query_tstamp = query_tstamp + TSstart + '" limit 10000) test'

    print 'Query = ' + query_tstamp + '\n'
    
    #get max timestamp
    try:
        cur.execute(query_tstamp)
    except:
        print '>> Error parsing timestamp database'
        
    data = cur.fetchall()

    print 'After Timestamp Query... 1'

    for row in data:
        TSend = row[0]

        if TSend != None:
            cfg = ConfigParser.ConfigParser()
            cfg.read('senslope-server-config.txt')
            #cfg.set('Misc', 'TimeStampEnd', TSend)
            cfg.set('Misc', ts_site, TSend)
            with open('senslope-server-config.txt', 'wb') as configfile:
                cfg.write(configfile)

            os.system(winCmd)
        else:
            print '>> Current TimeStampEnd is latest data or it is currently set to None'

        time.sleep(3)

    time.sleep(10)
    db.close()
    print 'done'


#def extractDBToSQL(table):
#    cfg = ConfigParser.ConfigParser()
#    cfg.read('senslope-server-config.txt')
#
#    ts_site = 'ts_' + table
#    print '>> ts_site = ' + ts_site
#	
#    # The new time start is the last TimeStampEnd
#    #TSstart = cfg.get('Misc', 'TimeStampEnd')
#    TSstart = cfg.get('Misc', ts_site)
#    
#    #table = 'labb'
#    tbase = dt.strptime('"2010-10-1 00:00:00"', '"%Y-%m-%d %H:%M:%S"')
#    print '>> Extracting ' + table + ' purged data from database ..\n'  
#
#    print 'TS Start = ' + TSstart + '\n'
#
#    tsStartParsed = re.sub('[.!,;:]', '', TSstart)
#    tsStartParsed = re.sub(' ', '_', tsStartParsed)
#    fileName = 'D:\\dewslandslide\\TESTF\\' + table + '_' + tsStartParsed + '.sql'
#
#    print 'filename parsed = ' + fileName + '\n'
#
#    winCmd = 'mysqldump -t -u ' + Userdb + ' -p' + Passdb + ' senslopedb ' + table
#    winCmd = winCmd + ' --where="timestamp > \'' + TSstart + '\'" > ' + fileName;
#
#    print 'winCmd = ' + winCmd + '\n'
#
#    db, cur = SenslopeDBConnect()
#    query_tstamp = 'select max(timestamp) from (SELECT timestamp FROM ' + table
#    query_tstamp = query_tstamp + ' where xvalue > 0 and zvalue > -500 '
#    query_tstamp = query_tstamp + 'and id > 0 and id < 41 and timestamp > "'
#    query_tstamp = query_tstamp + TSstart + '" limit 10000) test'
#
#    print 'Query = ' + query_tstamp + '\n'
#    
#    #get max timestamp
#    try:
#        cur.execute(query_tstamp)
#    except:
#        print '>> Error parsing timestamp database'
#        
#    data = cur.fetchall()
#
#    print 'After Timestamp Query... 1'
#
#    for row in data:
#        TSend = row[0]
#
#        if TSend != None:
#            cfg = ConfigParser.ConfigParser()
#            cfg.read('senslope-server-config.txt')
#            #cfg.set('Misc', 'TimeStampEnd', TSend)
#            cfg.set('Misc', ts_site, TSend)
#            with open('senslope-server-config.txt', 'wb') as configfile:
#                cfg.write(configfile)
#
#            os.system(winCmd)
#        else:
#            print '>> Current TimeStampEnd is latest data or it is currently set to None'
#
#        time.sleep(3)
#
#    time.sleep(10)
#    db.close()
#    print 'done'


def extract_db2():
    createUploadMarkerTable("upload_marker_accel")
    
    try:
        db, cur = SenslopeDBConnect()
        print '>> Connected to database'

        query = 'SELECT name FROM site_column WHERE installation_status = "Installed" ORDER BY s_id ASC'
        try:
            cur.execute(query)
        except:
            print '>> Error parsing database'
        
        data = cur.fetchall()

        for table in data:
            extractDBToSQL(table[0])

    except IndexError:
        print '>> Error in writing extracting database data to files..'

