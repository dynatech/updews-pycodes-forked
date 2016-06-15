"""
Created on Mon Jun 13 11:08:15 2016

@author: PradoArturo
"""

#!/usr/bin/python

import socket
import os
import sys
import time
import pandas as pd
#import datetime
from datetime import datetime
import queryPiDb as qpi

#import MySQLdb

#TODO: Add the accelerometer filter module you need to test
#import newAccelFilter as naf

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../Data Analysis'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs

def sendData(host, port, msg):
    s = socket.socket()    
    s.connect((host, port))
    print s.recv(1024)
    s.send(msg)
    s.close()

def writeToSMSoutbox(number, msg):
    try:
        qInsert = """INSERT INTO smsoutbox (recepients,sms_msg,send_status)
                    VALUES ('%s','%s','UNSENT');""" % (number,msg)
        print qInsert
        qpi.ExecuteQuery(qInsert)        
    
    except IndexError:
        print '>> Error in writing extracting database data to files..'


#host = "www.codesword.com"
host = "127.0.0.1"
#host1 = "192.168.1.100"
#port1 = 5051
pradoS = "09980619501"
pradoG = "09163677476"
ivy = "09065310825"
carloSun = "09228912093"
numbers = [pradoS, pradoG, ivy, carloSun]

def mainFunc():
    db, cur = qpi.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'
    
    #   sendData(host, port, columnName)
    i = datetime.now()
    txtmsg = "%s: Test text message from RPi" % (i)
    
    for number in numbers:
#        columnName = column[0]
#        sendData(host, port, columnName)
        print "%s: %s" % (number, txtmsg)
        writeToSMSoutbox(number, txtmsg)


#try:
#    db, cur = qs.SenslopeDBConnect('senslopedb')
#    print '>> Connected to database'
#
#    #Get all column names with installation status of "Installed"
#    queryColumns = 'SELECT name, version FROM site_column WHERE installation_status = "Installed" ORDER BY s_id ASC'
#    try:
#        cur.execute(queryColumns)
#    except:
#        print '>> Error parsing database'
#    
#    columns = cur.fetchall()
##    print columns
#
#    for column in columns:
#        columnName = column[0]
#        sendData(host, port, columnName)
#        print columnName
#
#except IndexError:
#    print '>> Error in writing extracting database data to files..'

mainFunc()