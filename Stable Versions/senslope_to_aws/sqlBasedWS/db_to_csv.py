import os,time,serial,re
import MySQLdb
import datetime
import ConfigParser
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

#def extractDBToSQL():
def extractDBToSQL(table):
    cfg = ConfigParser.ConfigParser()
    cfg.read('senslope-server-config.txt')

    ts_site = 'ts_' + table
    print '>> ts_site = ' + ts_site
	
    # The new time start is the last TimeStampEnd
    #TSstart = cfg.get('Misc', 'TimeStampEnd')
    TSstart = cfg.get('Misc', ts_site)
    
    #table = 'labb'
    tbase = dt.strptime('"2010-10-1 00:00:00"', '"%Y-%m-%d %H:%M:%S"')
    print '>> Extracting ' + table + ' purged data from database ..\n'  

    print 'TS Start = ' + TSstart + '\n'

    tsStartParsed = re.sub('[.!,;:]', '', TSstart)
    tsStartParsed = re.sub(' ', '_', tsStartParsed)
    fileName = 'D:\\dewslandslide\\' + table + '_' + tsStartParsed + '.sql'

    print 'filename parsed = ' + fileName + '\n'

    #mysqldump -t -u root -pirc311 senslopedb labb --where="timestamp > '2014-06-19 17:44'" > D:\labb.sql
    winCmd = 'mysqldump -t -u root -pirc311 senslopedb ' + table + ' --where="timestamp > \'' + TSstart + '\'" > ' + fileName;

    print 'winCmd = ' + winCmd + '\n'

    db, cur = SenslopeDBConnect()
    query_tstamp = 'select max(timestamp) from (SELECT timestamp FROM ' + table + ' where timestamp > "' + TSstart + '" limit 10000) test'

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

        valid_tables = ['blcw','bolw','gamw','humw','labw','lipw','mamw', \
                        'oslw','plaw','pugw','sinw','stats']
        for tbl in valid_tables:        
            extractDBToSQL(tbl)

        valid_arq_tables = ['agbtaw','baytcw','blcsaw','cudtaw','nagtbw',\
                            'pepsbw','sagtaw','tuetbw']
        for tbl2 in valid_arq_tables:        
            extractDBToSQL(tbl2)

        #extractDBToSQL('sinb')     
    except IndexError:
        print '>> Error in writing extracting database data to files..'

