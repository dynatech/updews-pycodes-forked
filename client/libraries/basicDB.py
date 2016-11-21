#import MySQLdb
import ConfigParser
from datetime import datetime as dtm
from datetime import timedelta as tda
import re
import json
import pandas.io.sql as psql
import pandas as pd
import numpy as np
import StringIO
import platform
import os
import sys

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

# Scripts for connecting to local database
# Needs config file: server-config.txt

def connectDB(nameDB=None):
    ctr = 0
    while True:
        try:
            if nameDB == None:
                db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb)
            else:
                db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=nameDB)
            cur = db.cursor()
            return db, cur
        except mysqlDriver.OperationalError:
            print '.',
            ctr = ctr + 1
            if ctr > 10:
                return None, None

def PrintOut(line):
    if printtostdout:
        print line

# Check if database schema exists
# Returns true if table exists
def DoesDatabaseSchemaExist(schema_name):
    db, cur = connectDB()
    cur.execute("SHOW DATABASES LIKE '%s'" %schema_name)

    if cur.rowcount > 0:
        db.close()
        return True
    else:
        db.close()
        return False

# Check if table exists
# Returns true if table exists
def DoesTableExist(schema_name, table_name):
    db, cur = connectDB(schema_name)
    cur.execute("use "+ schema_name)
    cur.execute("SHOW TABLES LIKE '%s'" %table_name)

    if cur.rowcount > 0:
        db.close()
        return True
    else:
        db.close()
        return False
        
# Get table Primary Key/s
def GetTablePKs(schema_name, table_name):
    query = """SHOW INDEX FROM %s""" % (table_name)
    return GetDBResultset(query, schema_name)
        
def CreateSchema(schema_name):
    query = "CREATE DATABASE IF NOT EXISTS %s" % (schema_name)
    ExecuteQuery(query)
        
def CreateMasyncSchemaTargetsTable(schema_name=None):
    db, cur = connectDB(schema_name)
    query = """
            CREATE TABLE `masynckaiser`.`masynckaiser_schema_targets` (
              `schema_id` INT NOT NULL AUTO_INCREMENT,
              `name` VARCHAR(64) NOT NULL,
              `for_sync` INT NULL DEFAULT 0,
              PRIMARY KEY (`schema_id`));    
            """    
    cur.execute(query)
    db.close()   

def CreateMasyncTablePermissionsTable(schema_name=None):
    db, cur = connectDB(schema_name)
    query = """
            CREATE TABLE `masynckaiser`.`masynckaiser_table_permissions` (
              `table_id` INT NOT NULL AUTO_INCREMENT,
              `schema_id` INT NOT NULL,
              `name` VARCHAR(45) NOT NULL,
              `sync_direction` VARCHAR(45) NOT NULL DEFAULT 0,
              PRIMARY KEY (`table_id`),
              INDEX `schema_id_idx` (`schema_id` ASC),
              CONSTRAINT `schema_id`
                FOREIGN KEY (`schema_id`)
                REFERENCES `masynckaiser`.`masynckaiser_schema_targets` (`schema_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE);  
            """    
    cur.execute(query)
    db.close()   
	
#GetDBResultset(query): executes a mysql like code "query"
#    Parameters:
#        query: str
#             mysql like query code
#    Returns:
#        resultset: str
#             result value of the query made
def GetDBResultset(query, schema_name=None):
    a = ''
    try:
        db, cur = connectDB(schema_name)
        a = cur.execute(query)
        db.commit()
        db.close()
    except mysqlDriver.Error, e:
        try:
            print "ERROR [%d]: %s" % (e.args[0], e.args[1])
            # print "ERROR: Exception detected [%s]" % (query)
            return e.args
        except IndexError:
            print "ERROR: %s" % str(e)

    if a:
        return cur.fetchall()
    else:
        return ""
        
#execute query without expecting a return
#used different name
def ExecuteQuery(query, schema_name=None):
    GetDBResultset(query, schema_name)
        
#Insert JSON Data directly to the database
#This function will do the parsing and query creation before inserting it to
#   the database
def PushDBjson(jsonData, table_name, schema_name=None, limit=100, insType="insert"):
    if insType == "replace":
        queryHeader = "REPLACE INTO %s " % (table_name)
    elif insType == "ignore":
        queryHeader = "INSERT IGNORE %s " % (table_name)
    else:
        queryHeader = "INSERT INTO %s " % (table_name)

    queryKeys = " ("
    queryValues = " VALUES "

    # Compose Query Keys
    for json_dict in jsonData:
        numElements = len(json_dict)
        ctr = 0
        for key,value in json_dict.iteritems():
            queryKeys = queryKeys + key
            ctr = ctr + 1

            if ctr < numElements:
                queryKeys = queryKeys + ","

        queryKeys = queryKeys + ")"
        break

    # Parse JSON data 
    #TODO: (limited by "limit" variable)
    numRows = len(jsonData)
    ctrRow = 0
    for json_dict in jsonData:
        numElements = len(json_dict)
        ctr = 0
        queryValues = queryValues + "("
        for key,value in json_dict.iteritems():
            # print("key: {} | value: {}".format(key, value))

            try:
                # Make sure to escape special characters
                esc_value = json.dumps(value)
                queryValues = queryValues + esc_value
            except TypeError:
                queryValues = queryValues + "null"

            ctr = ctr + 1
            if ctr < numElements:
                queryValues = queryValues + ","
            else:
                queryValues = queryValues + ")"

        ctrRow = ctrRow + 1
        if ctrRow < numRows:
            queryValues = queryValues + ","

    #Compose data insertion query
    query = queryHeader + queryKeys + queryValues
    # print "query: %s" % (query)

    #Execute the query
    return GetDBResultset(query, schema_name)


#GetDBDataFrame(query): queries a specific sensor data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
def GetDBDataFrame(query, schema_name=None):
    try:
        db, cur = connectDB(schema_name)
        df = psql.read_sql(query, db)
        db.close()
        return df
    except KeyboardInterrupt:
        PrintOut("ERROR: Exception detected in accessing database")
        
#Push a dataframe object into a table
def PushDBDataFrame(df, table_name, schema_name=None):     
    db, cur = connectDB(schema_name)
    # df.to_sql(con=db, name=table_name, if_exists='append', flavor='mysql')
    df.to_sql(con=db, name=table_name, if_exists='append', flavor=None)
    db.commit()
    db.close()

#Push a dataframe object into a table with Guard against duplicate entries
def PushDBDataFrameDupGuard(df, table_name, schema_name=None):
    db, cur = connectDB(schema_name)
    # df.to_sql(con=db, name=table_name, if_exists='append', flavor='mysql')

    for i in range(len(df)):
        try:
            df.iloc[i:i+1].to_sql(con=db, name=table_name, if_exists='append', flavor='mysql')
        except IntegrityError:
            # or any other action
            pass        

    db.commit()
    db.close()

def initMasyncTables():
    # Create table for storing information on which schemas the user would
    #   like to be syncronized from the server
    table = "masynckaiser_schema_targets"
    exists = DoesTableExist(Masyncdb, table)
    if not exists:
        print "TABLE %s: DOES NOT exist" % (table)
        print "...creating table: %s..." % (table)
        CreateMasyncSchemaTargetsTable(Masyncdb)
    else:
        print "TABLE %s: EXISTS" % (table)
 
    # Create table for storing information on what type of synchronization
    #   direction the user would like for the tables
    #   Ex. Server to client only (READ), Bidirectional
    table = "masynckaiser_table_permissions"
    exists = DoesTableExist(Masyncdb, table)
    if not exists:
        print "TABLE %s: DOES NOT exist" % (table)
        print "...creating table: %s..." % (table)
        CreateMasyncTablePermissionsTable(Masyncdb)
    else:
        print "TABLE %s: EXISTS" % (table)


def initMasynckaiser():
    test = DoesDatabaseSchemaExist(Masyncdb)
    
    if test:
        print "SCHEMA %s: EXISTS" % (Masyncdb)
        
        # check if necessary tables already exist
        initMasyncTables()
    else:
        print "SCHEMA %s: DOES NOT exist" % (Masyncdb)
        
        # create Masyncdb
        print "...creating schema: %s..." % (Masyncdb)
        query = "CREATE DATABASE %s" % (Masyncdb)
        ExecuteQuery(query)
        
        # create Masyncdb tables
        initMasyncTables()

            
# import values from config file
configFile = "server-config.txt"

# get real path of the current file being run
dir_path = os.path.dirname(os.path.realpath(__file__))

# construct platform independent path
full_path = os.path.join(dir_path, configFile)

cfg = ConfigParser.ConfigParser()

try:
    cfg.read(full_path)
    print "%s: Found %s" % (__file__, full_path)
    
    DBIOSect = "DB I/O"
    Hostdb = cfg.get(DBIOSect,'Hostdb')
    Userdb = cfg.get(DBIOSect,'Userdb')
    Passdb = cfg.get(DBIOSect,'Passdb')
    Masyncdb = cfg.get(DBIOSect,'NamedbMasync')
    printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')
    
    initMasynckaiser()
except:
    print "%s ERROR: Looking for server-config.txt" % (__file__)
    sys.exit(1)




