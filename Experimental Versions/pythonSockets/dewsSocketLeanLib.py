"""
Created on Mon Jun 13 11:08:15 2016

@author: PradoArturo
"""

#!/usr/bin/python

import socket
import os
import sys
import time
import json
import simplejson
import pandas as pd
#import datetime
from datetime import datetime
import queryPiDb as qpi

#Simple Python WebSocket
from websocket import create_connection

#import MySQLdb

#TODO: Add the accelerometer filter module you need to test
#import newAccelFilter as naf

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../Data Analysis'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs

###############################################################################
# GSM Functionalities
###############################################################################

def writeToSMSoutbox(number, msg, timestamp = None):
    try:
        print "timestamp: %s" % (timestamp)
        if timestamp != None:
            qInsert = """INSERT INTO smsoutbox (timestamp_sent,recepients,sms_msg,send_status)
                        VALUES ('%s','%s','%s','UNSENT');""" % (timestamp,number,msg)
        else:
            qInsert = """INSERT INTO smsoutbox (recepients,sms_msg,send_status)
                        VALUES ('%s','%s','UNSENT');""" % (number,msg)

        print qInsert
        qpi.ExecuteQuery(qInsert)
        return 0
    
    except IndexError:
        print '>> Error in writing extracting database data to files..'
        return -1

def sendMessageToGSM(recipients, msg, timestamp = None):
    db, cur = qpi.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'
    ctr = 0
    
    for number in recipients:
        # print "%s: %s" % (number, msg)
        writeStatus = writeToSMSoutbox(number, msg, timestamp)

        if writeStatus < 0:
            ctr -= 1

    return ctr

def sendTimestampToGSM(host, port, recipients):
    db, cur = qpi.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'
    
    i = datetime.now()
    txtmsg = "%s: Test text message from RPi" % (i)
    
    for number in recipients:
        sendDataFullCycle(host, port, txtmsg)
        print "%s: %s" % (number, txtmsg)
        writeToSMSoutbox(number, txtmsg)

###############################################################################
# Regular Sockets
###############################################################################

#Open a socket connection
def openSocketConn(host, port):
    s = socket.socket()
    s.connect((host, port))
    return s

#Close a socket connection
def closeSocketConn(sock_conn):
    sock_conn.close()

#One full cycle of opening connection
# sending data and closing connection
def sendDataFullCycle(host, port, msg):
    s = socket.socket()    
    s.connect((host, port))
    print msg
    s.send(msg)
    s.close()

def sendColumnNamesToSocket(host, port):
    try:
        db, cur = qs.SenslopeDBConnect('senslopedb')
        print '>> Connected to database'
    
        #Get all column names with installation status of "Installed"
        queryColumns = 'SELECT name, version FROM site_column WHERE installation_status = "Installed" ORDER BY s_id ASC'
        try:
            cur.execute(queryColumns)
        except:
            print '>> Error parsing database'
        
        columns = cur.fetchall()
    #    print columns
    
        for column in columns:
            columnName = column[0]
            sendDataFullCycle(host, port, columnName)
            print columnName
    
    except IndexError:
        print '>> Error in writing extracting database data to files..'

###############################################################################
# Web Sockets
###############################################################################

#One full cycle of opening connection
# sending data and closing connection
def sendDataToWSS(host, port, msg):
    try:
        ws = create_connection("ws://%s:%s" % (host, port))
#        print "Opened WebSocket"
#        print msg
        ws.send(msg)
        print "Sent %s" % (msg)
        ws.close()
#        print "Closed WebSocket"
        
        #returns 0 on successful sending of data
        return 0
    except:
        print "Failed to send data. Please check your internet connection"        
        
        #returns -1 on failure to send data
        return -1
    
#Connect to Web Socket Server
def connectWS(host, port):
    try:
        #create 
        ws = create_connection("ws://%s:%s" % (host, port))

        #returns 0 on successful sending of data
        return 0
    except:
        print "Failed to send data. Please check your internet connection"        
        
        #returns -1 on failure to send data
        return -1

#No filtering yet for special characters
def formatReceivedGSMtext(timestamp, sender, message):
    jsonText = """{"type":"smsrcv","timestamp":"%s","sender":"%s","msg":"%s"}""" % (timestamp, sender, message)
    return jsonText    
    
def sendDataToDEWS(msg):
#    host = "www.dewslandslide.com"
    host = "www.codesword.com"
    port = 5050
    success = sendDataToWSS(host, port, msg)
    return success

def sendReceivedGSMtoDEWS(timestamp, sender, message):
    jsonText = formatReceivedGSMtext(timestamp, sender, message)
    success = sendDataToDEWS(jsonText)
    return success

#Connect to WebSocket Server and attempt to reconnect when disconnected
#Receive and process messages as well
def connRecvReconn(host, port):
    url = "ws://%s:%s/" % (host, port)
    delay = 5

    while True:
        try:
            print "Receiving..."
            result = ws.recv()
            parseRecvMsg(result)
            # print "Received '%s'" % result
            delay = 5
        except Exception, e:
            # connectWS()
            try:
                print "Connecting to Websocket Server..."
                ws = create_connection(url)
            except Exception, e:
                print "Disconnected! will attempt reconnection in %s seconds..." % (delay)
                time.sleep(delay)

                if delay < 10:
                    delay += 1

    ws.close()

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
                ack_json = """{"type":"gsmack","timestamp":"%s","recipients":"%s","send_status":"FAIL"}""" % (timestamp, recipients)
                pass
            else:
                # if write SUCCESSFUL
                ack_json = """{"type":"gsmack","timestamp":"%s","recipients":"%s","send_status":"SENT"}""" % (timestamp, recipients)
                pass

            sendDataToDEWS(ack_json)
        elif commType == 'smsrcv':
            print "Warning: message type 'smsrcv', Message is ignored."
        else:
            print "Error: No message type detected. Can't send an SMS."
    except:
        print "Error: Please check the JSON construction of your message"









