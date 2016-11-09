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

#Get the Data Update from the Websocket Server
def getDataUpdateList(ws=None, schema=None, table=None, limit=10):
    if not ws or not schema or not table:
        print "%s ERROR: No ws|updateCmd value passed" % (common.whoami())
        return None

    latestPKval = getLatestPKValue(schema, table)
    updateCmd = masyncSR.getDataUpdateCommand(schema, table, latestPKval, limit)
    if updateCmd:    
        ws.send(updateCmd)
        result = ws.recv()
        # print "%s: Received '%s\n\n'" % (common.whoami(), result)
        dataUpdate = parseBasicList(result)
        
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
    for pk in primaryKeys:
        print "%s" % (pk[4]), 
        PKs.append(pk[4])
    
    if numPKs == 1:
        query = """
                SELECT %s 
                FROM %s 
                ORDER BY %s DESC 
                LIMIT 1""" % (primaryKeys[0][4], table, primaryKeys[0][4])
#        print "\n%s: %s" % (table, query)
        pkLatestValues = bdb.GetDBResultset(query, schema)
        
        #Construct json string
        jsonPKandValstring = '{"%s":"%s"}' % (PKs[0], pkLatestValues[0][0])
        jsonPKandVal = json.loads(jsonPKandValstring)
        print jsonPKandVal
        return jsonPKandVal
                
    elif numPKs > 1:
        #TODO: There is a different procedure for tables with multiple PKs
        print "\n%s: %s Number of Primary Keys: %s (TODO)" % (common.whoami(), table, numPKs)
        return -1

#Parse the json message and return as an array
def parseBasicList(payload):
    msg = format(payload.decode('utf8'))
    parsed_json = json.loads(json.loads(msg))
    
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
    
def parseRecvMsg(payload):
    msg = format(payload.decode('utf8'))
    print("Text message received: %s" % msg)

    #The local ubuntu server is expected to receive a JSON message
    #parse the numbers from the message
    try:
        parsed_json = json.loads(msg)
        commType = parsed_json['type']

        if commType == 'smssend':
            recipients = parsed_json['numbers']
            print "Recipients of Message: %s" % (len(recipients))
            
            message = parsed_json['msg']
            timestamp = parsed_json['timestamp']
            
            writeStatus = sendMessageToGSM(recipients, message, timestamp)

            # TODO: create a message containing the recipients, timestamp, and
            #   write status to raspi database
            if writeStatus < 0:
                # if write unsuccessful
                ack_json = """{"type":"ackrpi","timestamp_written":"%s","recipients":"%s","send_status":"FAIL"}""" % (timestamp, recipients)
                pass
            else:
                # if write SUCCESSFUL
                ack_json = """{"type":"ackrpi","timestamp_written":"%s","recipients":"%s","send_status":"SENT-PI"}""" % (timestamp, recipients)
                pass

            sendDataToDEWS(ack_json)
        elif commType == 'smsrcv':
            print "Warning: message type 'smsrcv', Message is ignored."
        else:
            print "Error: No message type detected. Can't send an SMS."
    except:
        print "Error: Please check the JSON construction of your message"