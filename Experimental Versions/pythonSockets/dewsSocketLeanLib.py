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

def writeToSMSoutbox(number, msg):
    try:
        qInsert = """INSERT INTO smsoutbox (recepients,sms_msg,send_status)
                    VALUES ('%s','%s','UNSENT');""" % (number,msg)
        print qInsert
        qpi.ExecuteQuery(qInsert)        
    
    except IndexError:
        print '>> Error in writing extracting database data to files..'

def sendMessageToGSM(recipients, msg):
    db, cur = qpi.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'
    
    for number in recipients:
        print "%s: %s" % (number, msg)
        writeToSMSoutbox(number, msg)

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
    
#No filtering yet for special characters
def formatReceivedGSMtext(smsid, timestamp, sender, message):
    jsonText = """{"type":"smsrcv","sms_id":"%s","timestamp":"%s","sender":"%s","msg":"%s"}""" % (smsid, timestamp, sender, message)
    return jsonText    
    
def sendDataToDEWS(msg):
#    host = "www.dewslandslide.com"
    host = "www.codesword.com"
    port = 5050
    sendDataToWSS(host, port, msg)
    
def sendReceivedGSMtoDEWS(smsid, timestamp, sender, message):
    jsonText = formatReceivedGSMtext(smsid, timestamp, sender, message)
    sendDataToDEWS(jsonText)















