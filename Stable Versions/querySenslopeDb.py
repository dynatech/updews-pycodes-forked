import MySQLdb
import ConfigParser
from datetime import datetime as dtm
from datetime import timedelta as tda
import re

# Scripts for connecting to local database
# Needs config file: server-config.txt

class columnArray:
    def __init__(self, name, number_of_segments, segment_length):
        self.name = name
        self.nos = number_of_segments
        self.seglen = segment_length     


def SenslopeDBConnect(nameDB):
    while True:
        try:
			db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=nameDB)
			cur = db.cursor()
			return db, cur
        except MySQLdb.OperationalError:
            print '.',

def PrintOut(line):
    if printtostdout:
        print line
		
def CreateTable(table_name, nameDB):
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
    cur.execute("USE %s"%nameDB)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, xvalue int, yvalue int, zvalue int, mvalue int, PRIMARY KEY (timestamp, id))" %table_name)
    db.close()
	
	
def GetDBResultset(query):
    a = ''
    try:
        db, cur = SenslopeDBConnect(Namedb)

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
	#try:
	db, cur = SenslopeDBConnect(Namedb)
	cur.execute("use "+ Namedb)
	
	query = 'SELECT name, num_nodes, seg_length FROM site_column_props inner join site_column on site_column_props.s_id=site_column.s_id'
	#try:
	cur.execute(query)
	#except:
	#print '>> Error parsing database'
	
	data = cur.fetchall()
	
	# make a sensor list of columnArray class functions
	sensors = []
	for entry in data:
		s = columnArray(entry[0],int(entry[1]),float(entry[2]))
		sensors.append(s)
	
	return sensors
	#except:
	#	print '>> Error getting list from database'
	#	return ''

    
# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

DBIOSect = "DB I/O"
Hostdb = cfg.get(DBIOSect,'Hostdb')
Userdb = cfg.get(DBIOSect,'Userdb')
Passdb = cfg.get(DBIOSect,'Passdb')
Namedb = cfg.get(DBIOSect,'Namedb')
NamedbPurged = cfg.get(DBIOSect,'NamedbPurged')
printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')

valueSect = 'Value Limits'
xlim = cfg.get(valueSect,'xlim')
ylim = cfg.get(valueSect,'ylim')
zlim = cfg.get(valueSect,'zlim')
xmax = cfg.get(valueSect,'xmax')
islimval = cfg.getboolean(valueSect,'LimitValues')







