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

#def extractDBToFile2():
def extractDBToFile2(table):
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
    fileName = 'D:\\csvForUpload\\' + table + '_' + tsStartParsed + '.csv'

    print 'filename parsed = ' + fileName + '\n'

    db, cur = SenslopeDBConnect()
    query = 'select * from ' + table + ' where xvalue > 0 and zvalue > -500 and id > 0 and id < 41 and timestamp >= "' + TSstart + '" order by timestamp asc limit 1000 '
    query_tstamp = 'select max(timestamp) from (SELECT timestamp FROM ' + table + ' where xvalue > 0 and zvalue > -500 and id > 0 and id < 41 and timestamp >= "' + TSstart + '" limit 1000)test'

    print 'Query = ' + query + '\n'
    
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
			
	    #query the new data
            try:
                cur.execute(query)
            except:
                print '>> Error exporting database to csv'

            data = cur.fetchall()
            print 'After Query... 2'

            fileNum = file(fileName, 'w')

            for row in data:
                x = float(row[1])

                # allow x values up to 1100 to be updated
                if x < 0 and x+4096 < 1126.0:
                    x = 1.0
        
                y = float(row[2])/1024.0
                z = float(row[3])/1024.0

                v = pow((pow(x,2) + pow(y,2) + pow(z,2)), 1/2)

                if v > 0.90 and v < 1.05:
                    tcur = row[0]
                    trec = tcur - tbase
                    trec = float(trec.days) + float(trec.seconds)/24.0/3600.0
                    trec = round(trec,6)
                    fileNum.write(row[0].strftime('"%Y-%m-%d %H:%M:%S"')+',')
                    fileNum.write(repr(trec)+',')

                    fileNum.write(repr(int(row[1]))+',')
                    
                    if x == 1.0:
                        fileNum.write("1023,")
                    else:
                        fileNum.write(repr(int(row[2]))+',')
                
                    for i in range(3,len(row)):
                        fileNum.write(repr(int(row[i]))+',')
                
                    fileNum.write('\n')
            fileNum.close()
			
        else:
            print '>> Current TimeStampEnd is latest data or it is currently set to None'

        time.sleep(3)
    
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

        #for tbl in data:
        #    if tbl[0] not in ["pugw","labw","sint","darq","abcd","axel","axl2","eee3","nigs","eeet","nlt1","ocim","outs","pott","sms1","smst","soms","strs","tbiz","temp","tesb","tim1","txt1","txt2","volt","watt","wha2","what"]:
        #        extractDBToFile(tbl[0])

        
        for tbl in data:
            if tbl[0] not in ["server_comm_pc","contact_community","contact_electric_provider","contact_lgu","contact_ngo","site_column","site_column_props","site_geology","site_piezo","pugw","labw","oslt","sint","darq","mico","mlkt","abcd","axel","axl2","eee3","nigs","eeet","nlt1","ocim","outs","pott","sms1","smst","soms","strs","tbiz","temp","tesb","tim1","txt1","txt2","volt","watt","wha2","what"]:
                extractDBToFile2(tbl[0])
        

        #extractDBToFile2("sinu")      

    ##    test = raw_input('>> End of Code: Press any key to exit')
    except IndexError:
        print '>> Error in writing extracting database data to files..'
    
