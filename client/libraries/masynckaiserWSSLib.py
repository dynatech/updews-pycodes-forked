"""
Created on Mon Oct 03 17:44:15 2016

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
from datetime import datetime

#Simple Python WebSocket
from websocket import create_connection

#include the path of "Data Analysis" folder for the python scripts searching
# path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../Data Analysis'))
# if not path in sys.path:
#     sys.path.insert(1,path)
# del path   

import basicDB as bdb
import masynckaiserServerRequests as masyncSR
import common

###############################################################################
# GSM Functionalities
###############################################################################

# Identify contact number's network
def identifyMobileNetwork(contactNumber):
    try:
        countNum = len(contactNumber);

        # //ex. 09 '16' 8888888
        if (countNum == 11) :
            # curSimPrefix = substr(contactNumber, 2, 2);
            curSimPrefix = contactNumber[2:4]
        # //ex. 639 '16' 8888888
        elif (countNum == 12) :
            # curSimPrefix = substr(contactNumber, 3, 2);
            curSimPrefix = contactNumber[3:5]

        print "Py simprefix: 09%s" % (curSimPrefix);
        # //TODO: compare the prefix to the list of sim prefixes

        # //Mix of Smart, Sun, Talk & Text
        networkSmart = "00,07,08,09,10,11,12,14,18,19,20,21,22,23,24,25,28,29,30,31,32,33,34,38,39,40,42,43,44,46,47,48,49,50,89,98,99"
        # //Mix of Globe and TM
        networkGlobe = "05,06,15,16,17,25,26,27,35,36,37,45,75,77,78,79,94,95,96,97"

        if networkSmart.find(curSimPrefix) >= 0:
            print "Py Smart Network!\n";
            return "SMART";
        elif networkGlobe.find(curSimPrefix) >= 0:
            print "Py Globe Network!\n";
            return "GLOBE";
        else:
            print "Py Unkown Network!\n"
            return "UNKNOWN"

    except:
        print "identifyMobileNetwork Exception: Unknown Network\n"
        return "UNKNOWN"

def writeToSMSoutbox(number, msg, timestamp = None, mobNetwork = None):
    try:
        mobNetwork = identifyMobileNetwork(number)
        print "timestamp: %s" % (timestamp)

        if timestamp != None:
            qInsert = """INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg,send_status,gsm_id)
                        VALUES ('%s','%s','%s','UNSENT','%s');""" % (timestamp,number,msg,mobNetwork)
        else:
            qInsert = """INSERT INTO smsoutbox (recepients,sms_msg,send_status,gsm_id)
                        VALUES ('%s','%s','UNSENT','%s');""" % (number,msg,mobNetwork)

        print qInsert
        bdb.ExecuteQuery(qInsert)
        return 0
    
    except IndexError:
        print '>> Error in writing extracting database data to files..'
        return -1

def getAllSMSoutbox(send_status='SENT',limit=20):
    try:
        query = """SELECT sms_id, timestamp_written, timestamp_sent, recepients 
            FROM smsoutbox
            WHERE send_status = '%s' 
            AND timestamp_written IS NOT NULL 
            AND timestamp_sent IS NOT NULL 
            ORDER BY sms_id ASC LIMIT %d""" % (send_status, limit)
            
        print query
        result = bdb.GetDBResultset(query)
        return result

    except MySQLdb.OperationalError:
        print 'getAllSMSoutbox DB Error',

def getAllSMSinbox(web_flag='W',read_status='READ-SUCCESS',limit=20):
    try:
        #sms_id, timestamp, sender, message
        query = """SELECT sms_id, timestamp, sim_num, sms_msg
                FROM smsinbox
                WHERE web_flag = '%s'
                AND read_status = '%s'
                ORDER BY sms_id ASC LIMIT %d """ % (web_flag, read_status, limit)

        print query
        result = bdb.GetDBResultset(query)
        return result

    except MySQLdb.OperationalError:
        print 'getAllSMSinbox DB Error',

def setSendStatus(send_status,sms_id_list):
    if len(sms_id_list) <= 0:
        return

    try:
        queryUpdate = "update smsoutbox set send_status = '%s' where sms_id in (%s) " % (send_status, str(sms_id_list)[1:-1].replace("L",""))
        print queryUpdate
        bdb.ExecuteQuery(queryUpdate)
        return 0
    except IndexError:
        print 'setSendStatus >> Error in writing extracting database data to files..'
        return -1

def setWebFlag(web_flag, sms_id_list):
    if len(sms_id_list) <= 0:
        return

    try:
        queryUpdate = "update smsinbox set web_flag = '%s' where sms_id in (%s)" % (web_flag, str(sms_id_list)[1:-1].replace("L",""))
        print queryUpdate
        bdb.ExecuteQuery(queryUpdate)
        return 0
    except IndexError:
        print 'setWebFlag >> Error in writing extracting database data to files..'
        return -1
    
def sendMessageToGSM(recipients, msg, timestamp = None):
    db, cur = bdb.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'
    ctr = 0
    
    for number in recipients:
        # print "%s: %s" % (number, msg)
        # Filter out characters (") and (\)
        message = filterSpecialCharacters(msg)
        writeStatus = writeToSMSoutbox(number, message, timestamp)

        if writeStatus < 0:
            ctr -= 1

    return ctr

def sendTimestampToGSM(host, port, recipients):
    db, cur = bdb.SenslopeDBConnect('senslopedb')
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
        db, cur = bdb.SenslopeDBConnect('senslopedb')
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
    
#No filtering yet for special characters
def formatAckSentGSMtext(ts_written, ts_sent, recipient):
    jsonText = """{"type":"ackgsm","timestamp_written":"%s","timestamp_sent":"%s","recipients":"%s"}""" % (ts_written, ts_sent, recipient)
    return jsonText   

def sendDataToDEWS(msg, port=None):
    host = "www.dewslandslide.com"
    # host = "www.dewslandslide.com"
    
    if port == None:
        port = 5055
    
    success = sendDataToWSS(host, port, msg)
    return success

def filterSpecialCharacters(message):
    # Filter out characters (") and (\)
    message = message.replace('\\','\\\\').replace('"', '\\"')
    
    # Filter single quote character (')
    message = message.replace("'","\\'")
    
    return message

def sendReceivedGSMtoDEWS(timestamp, sender, message, port=None):
    # Filter out characters (") and (\)
    message = filterSpecialCharacters(message)
    
    jsonText = formatReceivedGSMtext(timestamp, sender, message)
    success = sendDataToDEWS(jsonText, port)
    return success

# Send an acknowledgement message to DEWS Web Socket Server
#   to let it know that the message has been sent already by the GSM
def sendAckSentGSMtoDEWS(ts_written, ts_sent, recipient, port=None):
    jsonText = formatAckSentGSMtext(ts_written, ts_sent, recipient)
    success = sendDataToDEWS(jsonText, port)
    return success

# Send smsinbox messages to the Web Socket Server
# This is mostly used for contingency purposes only
def sendBatchReceivedGSMtoDEWS(host="www.dewslandslide.com", port=5055, limit=20):
    #Load smsinbox messages with web_flag = 'W' and read_status = 'READ-SUCCESS'
    allmsgs = getAllSMSinbox('W','READ-SUCCESS',limit)

    #Return if no messages were found
    if len(allmsgs) == 0:
        print "No smsinbox messages for batch sending"
        return

    try:
        #Connect to the web socket server
        ws = create_connection("ws://%s:%s" % (host, port))
        print "Successfully connected to ws://%s:%s" % (host, port)
        batchlist = []

        #Send all messages to the web socket server
        for msg in allmsgs:
            sms_id = msg[0]
            ts = msg[1]
            sim_num = msg[2]
            sms_msg = filterSpecialCharacters(msg[3])

            print "id:%s, ts:%s, sim_num:%s, sms_msg:%s" % (sms_id, ts, sim_num, sms_msg)

            #sms batch sending
            jsonBatchSendText = formatReceivedGSMtext(ts, sim_num, sms_msg)
            ws.send(jsonBatchSendText)
            batchlist.append(sms_id)

        #Close connection to the web socket server
        ws.close()
        print "Successfully closed WSS connection"

        #Change the web flag status to "WSS" for successful sending
        setWebFlag("WSS",batchlist)

        #returns 0 on successful sending of data
        return 0
    except:
        print "Failed to send data. Please check your internet connection"        
        
        #returns -1 on failure to send data
        return -1
    

# Send Acknowledgement for ALL outbox sms with send_status "SEND"
#   to DEWS Web Socket Server
def sendAllAckSentGSMtoDEWS(host="www.dewslandslide.com", port=5055, limit=20):
    #Load all sms messages with "SENT" status
    allmsgs = getAllSMSoutbox('SENT',limit)

    #Return if no messages were found
    if len(allmsgs) == 0:
        print "No smsoutbox messages for acknowledgement"
        return

    try:     
        #Connect to the web socket server  
        ws = create_connection("ws://%s:%s" % (host, port))
        print "Successfully connected to ws://%s:%s" % (host, port)
        acklist = []

        #Send all messages to the web socket server
        for msg in allmsgs:
            sms_id = msg[0]
            ts_written = msg[1]
            ts_sent = msg[2]
            sim_num = msg[3]

            print "id:%s, ts_written:%s, ts_sent:%s, sim_num:%s" % (sms_id, ts_written, ts_sent, sim_num)

            #send acknowledgement message
            jsonAckText = formatAckSentGSMtext(ts_written, ts_sent, sim_num)
            ws.send(jsonAckText)
            acklist.append(sms_id)

        #Close conenction to the web socket server
        ws.close()
        print "Successfully closed WSS connection"

        #Change the send status to "SENT-WSS" for successful sending
        setSendStatus("SENT-WSS",acklist)
        
        #returns 0 on successful sending of data
        return 0
    except:
        print "Failed to send data. Please check your internet connection"        
        
        #returns -1 on failure to send data
        return -1

#Connect to WebSocket Server and attempt to reconnect when disconnected
#Receive and process messages as well
def connRecvReconn(host, port):
    url = "ws://%s:%s/" % (host, port)
    delay = 5

    # TODO: We need to do multithreading or multiprocessing on this part in
    #   order to run "sending" and "receiving" asynchronously

    while True:
        try:
            print "Receiving..."
            result = ws.recv()
            parseRecvMsg(result)
            # print "Received '%s'" % result
            delay = 5
        except Exception, e:
            try:
                print "Connecting to Websocket Server..."
                ws = create_connection(url)
            except Exception, e:
                print "Disconnected! will attempt reconnection in %s seconds..." % (delay)
                time.sleep(delay)

                if delay < 10:
                    delay += 1

    ws.close()

#Test Send data and receive response from server
def testSendRecv(host, port, schema=None, table=None):
    url = "ws://%s:%s/" % (host, port)
    
    print "Connecting to Websocket Server..."
    ws = create_connection(url)
    
    requestMsg = masyncSR.showSchemas()
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
        print "%s: Received '%s'" % (common.whoami(), result)
    
    requestMsg = masyncSR.showTables(schema)
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
        print "%s: Received '%s'" % (common.whoami(), result)
    
    requestMsg = masyncSR.getTableConstructionCommand(schema, table)
    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
        print "%s: Received '%s'" % (common.whoami(), result)
    
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









