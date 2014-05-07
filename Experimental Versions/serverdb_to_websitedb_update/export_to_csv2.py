import os,time,serial,re
import MySQLdb
import datetime
import ConfigParser, sys
from datetime import datetime as dt
from extract_data_from_db import *
from format_column_data import *
from extract_rain_data import *
from db_to_csv import *

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
   
def runBackup():
    print '>> Backing up Database..',
    try:
        query = 'mysqldump -h '+Hostdb+' -u '+Userdb+' -p'+\
                  Passdb+' '+Namedb+' > '+Namedb+'-backup.sql'
        os.system(query)
    except:
        print '>> Error backing up database'
    else:
        print 'done'

    print '>> Updating csv files..'
    extract_db()
    #format_data()
    #extract_rain()
    print 'done'

    return dt.today()


""" Global variables"""
cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')
SleepPeriod = cfg.getint('Misc','SleepPeriod')

def main():

    print time.asctime()
    extract_db2()

    print "Sleep.."
    time.sleep(180)

    #test = raw_input('>> End of Code: Press any key to exit')

   
if __name__ == '__main__':
    #setup(console=["all_receiver2.py"])
    while True:
        try:
            main()
        except KeyboardInterrupt:
            gsm.close()
            print '>> Exiting gracefully.'
        except:
            print time.asctime()
            print "Unexpected error:", sys.exc_info()[0]
            
        
