import os,time,re
#import serial
import MySQLdb
import datetime
import ConfigParser
import platform
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
    
""" Global variables"""
cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')
SleepPeriod = cfg.getint('Misc','SleepPeriod')

currentos = platform.system()

if currentos == 'Windows':
    print "Current OS is Windows"
    FDirectory = cfg.get('FilePath', 'directory')
    TimestampDirectory = cfg.get('FilePath', 'timestamplatest')
elif currentos == 'Linux':
    print "Current OS is Linux"
    FDirectory = cfg.get('FilePath', 'awsdump')
    TimestampDirectory = cfg.get('FilePath', 'awstimestamplatest')
else:
    print "OS is not Windows or Linux"

#def extractDBToSQL():
def extractDBToSQL(table):
    cfg = ConfigParser.ConfigParser()
    cfg.read(TimestampDirectory)

    ts_site = 'ts_' + table
    print '>> ts_site = ' + ts_site

    TSstart = cfg.get('Misc', ts_site)
    
    tbase = dt.strptime('"2010-10-1 00:00:00"', '"%Y-%m-%d %H:%M:%S"')
    print '>> Extracting ' + table + ' purged data from database ..\n'  

    print 'TS Start = ' + TSstart + '\n'

    tsStartParsed = re.sub('[.!,;:]', '', TSstart)
    tsStartParsed = re.sub(' ', '_', tsStartParsed)
    #fileName = 'D:\\dewslandslide\\' + table + '_' + tsStartParsed + '.sql'
    fileName = FDirectory + table + '_' + tsStartParsed + '.sql'

    print 'filename parsed = ' + fileName + '\n'

    db, cur = SenslopeDBConnect()
    query_tstamp = 'select max(timestamp) from (SELECT timestamp FROM ' + table + ' where xvalue > 0 and zvalue > -500 and id > 0 and id < 41 and timestamp > "' + TSstart + '" order by timestamp desc limit 1) test'

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
            #cfg.read('timestamps-config.txt')
            cfg.read(TimestampDirectory)
            cfg.set('Misc', ts_site, TSend)
            #with open('timestamps-config.txt', 'wb') as configfile:
            with open(TimestampDirectory, 'wb') as configfile:
                cfg.write(configfile)

        else:
            print '>> Current TimeStampEnd is latest data or it is currently set to None'

        time.sleep(2)

    time.sleep(3)
    db.close()
    print 'done'

#import the sql files from the sql dumps folder
def importSQLtoDB():
    print 'Check if sql dumps directory is empty'
    
    sqldumps = os.listdir(FDirectory)
    
    if sqldumps == []:
        print "Directory is empty. No sql files to import."
    else:
        print "Contains Files... Beginning sql files importation..."
        
        for sitesql in sqldumps:
            print sitesql
            cmdout = os.system("mysql -u " + Userdb + " -p" + Passdb + " " + Namedb + " < " + FDirectory + "\\" + sitesql)        

            if cmdout:
                print "please check if " + sitesql + " is corrupted!"
            else:
                print sitesql + " has been imported to the local database..."
                os.remove(FDirectory + "\\" + sitesql)
#            
#            TSend = row[0]
#    
#            if TSend != None:
#                cfg = ConfigParser.ConfigParser()
#                #cfg.read('timestamps-config.txt')
#                cfg.read(TimestampDirectory)
#                cfg.set('Misc', ts_site, TSend)
#                #with open('timestamps-config.txt', 'wb') as configfile:
#                with open(TimestampDirectory, 'wb') as configfile:
#                    cfg.write(configfile)
#    
#            else:
#                print '>> Current TimeStampEnd is latest data or it is currently set to None'
#    
            time.sleep(2)
    
        time.sleep(3)
    print "Done with sql importation"

def extract_db2():
    
    try:
        db, cur = SenslopeDBConnect()
        print '>> Connected to database'


        query = 'select TABLE_NAME from information_schema.tables where TABLE_SCHEMA = "' + Namedb + '"'
        try:
            cur.execute(query)
        except:
            print '>> Error parsing database'
        
        data = cur.fetchall()

        #import the sql dumps from the Web Server to the local database
        importSQLtoDB()

        valid_tables = ['blcb','blct','bolb','gamt','gamb','humt','humb','labb','labt','lipb','lipt','mamb','mamt','oslb','oslt','plab','plat','pugb','pugt','sinb','sinu']
        for tbl in valid_tables:        
            extractDBToSQL(tbl)

        #extractDBToSQL('sinb')     
    except IndexError:
        print '>> Error in writing extracting database data to files..'

