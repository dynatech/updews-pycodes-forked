import socket
import os
import sys
import thread
import time
import json
import simplejson
import pandas as pd
from datetime import datetime

#Simple Python WebSocket
from websocket import create_connection

import basicDB as bdb
import common
import masynckaiserServerRequests as masyncSR

#Get the schema list from the Websocket Server
def getSchemaList(ws=None):
    if not ws:
        print "%s ERROR: No ws value passed" % (common.whoami())
        return None
    
    requestMsg = masyncSR.showSchemas()
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
#        print "%s: Received '%s'" % (common.whoami(), result)
        schemaList = parseBasicList(result)
        
        return schemaList
    else:
        print "%s ERROR: No request message passed" % (common.whoami())
        return None

#Get the tables list from the Websocket Server
def getTableList(ws=None, schema=None):
    if not ws or not schema:
        print "%s ERROR: No ws|schema value passed" % (common.whoami())
        return None
    
    requestMsg = masyncSR.showTables(schema)
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
#        print "%s: Received '%s\n\n'" % (common.whoami(), result)
        tableList = parseBasicList(result)
        
        return tableList
    else:
        print "%s ERROR: No request message passed" % (common.whoami())
        return None

#Get the table creation command from the Websocket Server
def getTableCreationCmd(ws=None, schema=None, table=None):
    if not ws or not schema or not table:
        print "%s ERROR: No ws|schema|table value passed" % (common.whoami())
        return None
    
    requestMsg = masyncSR.getTableConstructionCommand(schema, table)
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
#        print "%s: Received '%s\n\n'" % (common.whoami(), result)
        tableCreationCommand = parseTableCreationCommand(result)
        
        return tableCreationCommand
    else:
        print "%s ERROR: No request message passed" % (common.whoami())
        return None

#Find the existence of table in the Web Socket Server
def findTableExistence(ws=None, schema=None, table=None):
    if not ws or not schema or not table:
        print "%s ERROR: No ws|schema|table value passed" % (common.whoami())
        return None
    
    requestMsg = masyncSR.findTable(schema, table)
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
#        print "%s: Received '%s\n\n'" % (common.whoami(), result)
        doesTableExist = len(parseBasicList(result))
        
        return doesTableExist
    else:
        print "%s ERROR: No request message passed" % (common.whoami())
        return None

#Get the Data Update from the Websocket Server
def getDataUpdateList(ws=None, schema=None, table=None, limit=10, withKey=True):
    if not ws or not schema or not table:
        print "%s ERROR: No ws|updateCmd value passed" % (common.whoami())
        return None

    latestPKval = getLatestPKValue(schema, table)

    try:
        #Catch mismatched table construction
        if latestPKval[0] == 1146:
            return latestPKval
    except Exception as e:
        pass

    #TEMPORARY: catch if no PKval was returned
    if latestPKval == -1:
        print "%s TESTING: multiple primary keys in a table" % (common.whoami())
        return None

    updateCmd = masyncSR.getDataUpdateCommand(schema, table, latestPKval, limit)
    if updateCmd:    
        ws.send(updateCmd)
        result = ws.recv()
        # print "%s: Received '%s\n\n'" % (common.whoami(), result)
        dataUpdate = parseBasicList(result, withKey)
        
        # Return data update
        return dataUpdate
    else:
        print "%s ERROR: No request message passed" % (common.whoami())
        return None

#Get the latest value of Primary Key/s of the client's database
def getLatestPKValue(schema, table):
    primaryKeys = bdb.GetTablePKs(schema, table)
    numPKs = len(primaryKeys)    
    print "\n%s %s: Number of Primary Keys: %s" % (common.whoami(), table, numPKs)
    
    print "%s:" % (table),
    PKs = []
    try:
        for pk in primaryKeys:
            print "%s" % (pk[4]), 
            PKs.append(pk[4])
    except:
        errorDetails = primaryKeys
        return errorDetails
    
    if numPKs == 1:
        return constructPKjson(schema, table, PKs[0])

    elif numPKs > 1 and numPKs < 4:
        #Identify the Main Primary Key (usually the timestamp)
        mainPK = getPKwithMostCount(schema, table, PKs)
        return constructPKjson(schema, table, mainPK)

    else:
        #There is a different procedure for tables with multiple PKs greater than 3
        # print "\n(TODO) %s: %s Number of Primary Keys: %s" % (common.whoami(), table, numPKs)
        # return -1
        countTS = 0
        countID = 0
        pkTS = []
        pkID = []

        for pk in PKs:
            #Check if there is a key with the word "timestamp" on it and use it as PK
            if pk.find("timestamp") >= 0:
                # print "%s: %s Use %s as Primary Key" % (common.whoami(), table, pk)
                #contruct the PK Json using the timestamp as primary key
                pkTS.append(pk)

            #Check if there is a key with the word "id" on it and use it as PK
            if pk.find("id") >= 0:
                # print "%s: %s Use %s as Primary Key" % (common.whoami(), table, pk)
                #contruct the PK Json using the id as primary key
                pkTS.append(pk)

        if len(pkTS) > 0:
            #Identify the Main Primary Key (usually the timestamp)
            mainPK = getPKwithMostCount(schema, table, pkTS)
            mainPKjson = constructPKjson(schema, table, mainPK)
            # print "%s: Main Primary Key JSON (%s)" % (common.whoami(), mainPKjson)
            return mainPKjson
        elif len(pkID) > 0:
            #Identify the Main Primary Key (use ID if the timestamp is unavailable)
            mainPK = getPKwithMostCount(schema, table, pkID)
            mainPKjson = constructPKjson(schema, table, mainPK)
            # print "%s: Main Primary Key JSON (%s)" % (common.whoami(), mainPKjson)
            return mainPKjson
        else:
            print "%s ERROR: No Main Primary Key Found for %s!" % (common.whoami(), table)
            return -1

def constructPKjson(schema, table, pKey):
        #Get the latest value of Main PK
        query = """
                SELECT %s 
                FROM %s 
                ORDER BY %s DESC 
                LIMIT 1""" % (pKey, table, pKey)
        pkLatestValues = bdb.GetDBResultset(query, schema)

        #Construct json string
        try:
            jsonPKandValstring = '{"%s":"%s"}' % (pKey, pkLatestValues[0][0])
        except IndexError:
            jsonPKandValstring = '{"%s":null}' % (pKey)

        #Return JSON PK and Value/s
        jsonPKandVal = json.loads(jsonPKandValstring)
        print "%s: %s" % (common.whoami(), jsonPKandVal)
        return jsonPKandVal

def getPKwithMostCount(schema, table, pKeys):
    if len(pKeys) <= 0:
        print "%s ERROR: No Primary Keys Found for %s" % (common.whoami(), table)
        return -1

    curBiggestPKcount = 0
    mainPK = ""

    for pk in pKeys:
        query = "SELECT COUNT(DISTINCT %s) FROM %s" % (pk, table)
        PKcount = bdb.GetDBResultset(query, schema)

        try:
            # print "%s %s.%s count = %s" % (common.whoami(), table, pk, PKcount)
            if PKcount > curBiggestPKcount:
                curBiggestPKcount = PKcount
                mainPK = pk
                # print "Main PK is now (%s)" % (mainPK)
        except:
            print "%s ERROR: Crash happened in counting for %s" % (common.whoami(), pk)

    #Return the mainPK 
    return mainPK

#Parse the json message and return as an array
def parseBasicList(payload, withKey=False):
    msg = format(payload.decode('utf8'))
    parsed_json = json.loads(json.loads(msg))
    
    if withKey:
        return parsed_json
    else:
        schemaList = []
        for json_dict in parsed_json:
            for key,value in json_dict.iteritems():
    #            print("key: {} | value: {}".format(key, value))
                schemaList.append(value)
                
        return schemaList

def parseTableCreationCommand(payload):
    msg = format(payload.decode('utf8'))
    parsed_json = json.loads(json.loads(msg))
    
    schemaList = []
    for json_dict in parsed_json:
        for key,value in json_dict.iteritems():
#            print("key: {} | value: {}".format(key, value))
            schemaList.append(value)
            
#    print schemaList[1]
    return schemaList[1]