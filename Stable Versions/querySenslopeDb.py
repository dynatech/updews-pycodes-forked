import MySQLdb
import ConfigParser
from datetime import datetime as dtm
from datetime import timedelta as tda
import re

# Scripts for connecting to local database
# Needs config file: server-config.txt

def SenslopeDBConnect():
    while True:
        try:
            db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb, db = Namedb)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
            print '.',

def PrintOut(line):
    if printtostdout:
        print line

def GetDBResultset(query):
    a = ''
    try:
        db, cur = SenslopeDBConnect()

        a = cur.execute(query)

        db.close()
    except:
        PrintOut("Exception detected")

    if a:
        return cur.fetchall()
    else:
        return ""

def GetRawColumnData(siteid = "", fromTime = ""):

    if not siteid:
        raise ValueError('no site id entered')
    
    if printtostdout:
        PrintOut('Querying database ...')

    query = "select * from " + \
            Namedb + "." + \
            siteid + " where"

    if fromTime:
        query = query + " timestamp > '" + fromTime + "' and "

    query = query + \
            " (xvalue > " + xlim + \
            " or xvalue + 4096 < " + xmax + \
            ") and yvalue > -" + ylim + \
            " and yvalue < " + ylim + \
            " and zvalue > -" + zlim + \
            " and zvalue < " + zlim + \
            " and id > 0 and id < 41" + \
            ";"
##    return query
    return GetDBResultset(query)

def GetSensorList():
    try:
        db, cur = SenslopeDBConnect()

        query = 'select name from ' + Namedb + '.site_column order by name;'

        try:
            cur.execute(query)
        except:
            print '>> Error parsing database'
        
        data = cur.fetchall()

        return data
    except:
        print '>> Error getting list from database'
        return ''

    
# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

DBIOSect = "DB I/O"
Hostdb = cfg.get(DBIOSect,'Hostdb')
Userdb = cfg.get(DBIOSect,'Userdb')
Passdb = cfg.get(DBIOSect,'Passdb')
Namedb = cfg.get(DBIOSect,'Namedb')
printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')

valueSect = 'Value Limits'
xlim = cfg.get(valueSect,'xlim')
ylim = cfg.get(valueSect,'ylim')
zlim = cfg.get(valueSect,'zlim')
xmax = cfg.get(valueSect,'xmax')
islimval = cfg.getboolean(valueSect,'LimitValues')






