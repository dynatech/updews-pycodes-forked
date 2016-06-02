# -*- coding: utf-8 -*-
"""
Created on Thu May 19 13:56:10 2016

@author: kennex
"""

import MySQLdb as sql
import pandas.io.sql as psql
import ConfigParser
import pandas as pd
from datetime import datetime as dtm
# create_NodeAccelList(df,db,SC)
#    df - dataframe
#        dataframe that contains name of column
#        and number of nodes
#    db
#        db = sql.connect('localhost','root','senslope')
#    SC - pandas Object
#        the output of the function get_SC
#        essentially a groupby object that contains
#        the list of special cases

def create_NodeAccelTable(df,db,version,SC):
    for name in df.name:
        count = int(df.num_nodes[df['name'] == name])
        for node in range(1,count+1):
            put_entries(name,node,version,db,SC)

#put_entries(name,count,db,SC)
#    name - string
#        column name
#    count - integer
#        count is the node id being written
#    db
#        db = sql.connect('localhost','root','senslope')
#    SC - pandas Object
#        the output of the function get_SC
#        essentially a groupby object that contains
#        the list of special cases    
def put_entries(name,count,version,db,SC):
    accel = 1
    ts = '2010-01-01 00:00:00'
    
    query = "INSERT INTO node_accel_table(site_name,node_id,version,accel,last_changed) "
    if name in SC.column.unique():
        if count in SC.get_group(name).node.values:
            accel = 2
            ts = dtm.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            accel = 1
            ts = '2010-01-01 00:00:00'
            
    if name in list(version.get_group(2)['site']):
        vers = 2
    elif name in list(version.get_group(3)['site']):
        vers = 3
    elif (len(name) == 4):
        vers = 1
    else:
        vers = 3 #default
        
    query = query + "VALUES ('%s','%d','%d','%d','%s');" %(name,count,vers,accel,ts)
    cursor = db.cursor()
    cursor.execute(query)
    db.commit()
    print query

def update_NodeAccelTable(df,db,SC):
    for name in SC.column.unique():
        # name outputs something like ['carsb']
        # thus name[0] is used to get the array entry.
        for node in list(SC.get_group(name[0])['node']):
            query = "INSERT INTO node_accel_table(site_name,node_id,accel,last_changed) "
            accel = 2
            ts = dtm.now().strftime("%Y-%m-%d %H:%M:%S")
            query = query + "VALUES ('%s','%d','%d','%s');" %(name[0],node,accel,ts)
            cursor = db.cursor()
            print query
            cursor.execute(query)
            db.commit()
#get_SC(filename)
#    filename - string
#        name of .txt file to be read
#        usually config file
#    return - a pandas object after groupby
#        a dataframe after groupby
#        the dataframe is grouped by column
    
def get_SC(filename):   
    configFile = filename
    cfg = ConfigParser.ConfigParser()
    cfg.read(configFile)
    SCnodes=[]
    SpecialCase = 'SpecialCase'
    SCnodes = cfg.get(SpecialCase,'SCnodes')
    SCnodes = SCnodes.split(',')
    sc = []
    for i in range (0,len(SCnodes)):
        sc_entry1 = str(SCnodes[i])[0:5]
        sc_entry2 = int(str(SCnodes[i])[5:7])
        sc.append([sc_entry1,sc_entry2])
    SC = pd.DataFrame(sc)
    SC.columns = ('column','node')
    SC = SC.groupby('column')
    return SC
    
def get_version(filename):
    configFile = filename
    cfg = ConfigParser.ConfigParser()
    cfg.read(configFile)
    section = 'Version'
    v2 = cfg.get(section,'Version2')
    v3 = cfg.get(section,'Version3')
    v2 = v2.split(',')
    v3 = v3.split(',')
    version = []
    for i in range (0,len(v2)):
        v_entry1 = str(v2[i])[0:5]
        version.append([v_entry1,2])
    for i in range (0,len(v3)):
        v_entry1 = str(v3[i])[0:5]
        version.append([v_entry1,3])
    Version = pd.DataFrame(version)
    Version.columns = ('site','version')
    Version = Version.groupby('version')
    return Version


configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
print ' Start'
try:    
    cfg.read(configFile)
    
    DBIOSect = 'DB I/O'
    Hostdb = cfg.get(DBIOSect,'Hostdb')
    Userdb = cfg.get(DBIOSect,'Userdb')
    Passdb = cfg.get(DBIOSect,'Passdb')
    Namedb = cfg.get(DBIOSect,'Namedb')
    print 'Success at reading %s' %configFile
    	#db = sql.connect('localhost','root','senslope')
    try:
        db = sql.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)
        print 'db done'
    except sql.OperationalError:
        print "Can't connect to Database"

except:
    print "Can't connect to Database"
    print "user = root"
    print "password = senslope"
    print "db = sql.connect('localhost','root','senslope')"
    print "cursor = db.cursor()"


	
## select senslopedb
cursor = db.cursor()
query = 'USE senslopedb;'
cursor.execute(query)

# get list of tables
query = 'SELECT name,num_nodes FROM senslopedb.site_column_props'
df = psql.read_sql(query,db)
print query

# get SC (SpecialCase)
SC = get_SC("special_case_nodes.txt")
version = get_version("special_case_nodes.txt")
try:
    query = """create table node_accel_table (ac_id int auto_increment, site_name char(5) NOT NULL,node_id int,version int, accel int,last_changed datetime,primary key(ac_id));"""
    print query
    cursor.execute(query)
    db.commit()
    create_NodeAccelTable(df,db,version,SC)
    
except:
    print 'Updating Table. . .'
    update_NodeAccelTable(df,db,SC)   

db.close()