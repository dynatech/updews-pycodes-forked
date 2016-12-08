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

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

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
        schemaList = common.parseBasicList(result)
        
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
        tableList = common.parseBasicList(result)
        
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
        tableCreationCommand = common.parseTableCreationCommand(result)
        
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
        doesTableExist = len(common.parseBasicList(result))
        
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
        dataUpdate = common.parseBasicList(result, withKey)
        
        # Return data update
        return dataUpdate
    else:
        print "%s ERROR: No request message passed" % (common.whoami())
        return None

# Compare PK Values of Special Client (localhost) and Websocket Server
# Returns:
#       False - if there is no need to update the websocket server
#       Primary Key and the WSS Max Value - if the websocket server needs
#               to be updated
def comparePKValuesSCandWSS(ws=None, schema=None, table=None):
    # Get latest PK Value from Local Server
    mainPKandVal = getLatestPKValue(schema, table)

    # TODO: Check latest value available on WSS
    mainPK = None
    pkValLocalMax = None

    for key, value in mainPKandVal.iteritems():
        mainPK = key 
        pkValLocalMax = value

    qWSSPKval = "SELECT MAX(%s) as %s FROM %s" % (mainPK, mainPK, table)
    # print "WSS Query: %s" % (qWSSPKval)
    wsspkvalCmd = masyncSR.compReadQuery(schema, qWSSPKval)
    # print wsspkvalCmd
    if wsspkvalCmd:
        ws.send(wsspkvalCmd)
        result = ws.recv()
        # print "%s: Received '%s\n\n'" % (common.whoami(), result)
        dataWSSpkval = (common.parseBasicList(result, True))[0]
        print dataWSSpkval

        # Compare latest PK Value from Local Server and WSS
        pkValWSS = None
        for key, value in dataWSSpkval.iteritems():
            pkValWSS = value

        if pkValLocalMax > pkValWSS:
            # print "Local Data is more updated. Update Websocket Server data"
            return dataWSSpkval
        else:
            print "%s: No need to update Websocket Server data using Special Client" % (table)
            return False

# Get Latest Data from localhost that will be transferred to the Websocket Server
def getInsertQueryForServerTX(ws=None, schema=None, table=None, limit=10):
    # Compare PK Values of Special Client (localhost) and Websocket Server
    wssPKandVal = comparePKValuesSCandWSS(ws, schema, table)

    # Collect latest data to be transferred to WSS from Special Client
    if wssPKandVal:
        mainPK = None
        pkValLocalMax = None
        for key, value in wssPKandVal.iteritems():
            mainPK = key 
            pkValLocalMax = value

        if pkValLocalMax:
            qGetLocalData = "SELECT * FROM %s WHERE %s >= '%s' LIMIT %s" % (table, mainPK, pkValLocalMax, limit)
        else:
            qGetLocalData = "SELECT * FROM %s LIMIT %s" % (table, limit)

        # print "Query: %s" % (qGetLocalData)
        result = bdb.GetDBResultset(qGetLocalData, schema)
        fullData = json.dumps(result, cls=DateTimeEncoder)

        # Compose SQL to be sent to the WSS from Local Data Gathered
        queryHeader = "REPLACE INTO %s " % (table)
        queryValues = "VALUES "

        ctrRow = 0
        numRows = len(result)
        for data in result:
            ctr = 0
            numElements = len(data)
            queryValues = queryValues + "("
            for value in data:
                try:
                    # TODO: Make sure to escape special characters
                    # test = "%s" % (value)
                    # esc_value = json.dumps(test)
                    # queryValues = queryValues + esc_value
                    queryValues = queryValues + "'%s'" % (value)
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

        # Compose data insertion query
        query = queryHeader + queryValues
        # return query

        # Transfer data from Special Client to WSS
        requestMsg = masyncSR.modifierQuery(schema, query)
        # print requestMsg

        if requestMsg:    
            ws.send(requestMsg)
            result = ws.recv()
            # print "Result: %s" % (result)

            if result == "false":
                print "Table (%s) writing data on Web Server Failed" % (table)
                # return False
            elif result == "true":
                print "Table (%s) writing data on Web Server SUCCEEDED!" % (table)
                # return True
                getInsertQueryForServerTX(ws, schema, table, limit)


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