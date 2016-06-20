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
import dewsSocketLib as dsl
import json

#import MySQLdb

#TODO: Add the accelerometer filter module you need to test
#import newAccelFilter as naf

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../Data Analysis'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs

host = "www.codesword.com"
#host = "127.0.0.1"
#host = socket.gethostname()
port = 5051

pradoS = "09980619501"
pradoG = "09163677476"
ivy = "09065310825"
carloSun = "09228912093"
biboy = "09266413958"
kennex = "09293175812"
meryllS = "09228776515"
meryllG = "09068386258"
#numbers = [pradoS, pradoG, ivy, carloSun]
numbers = [pradoS]

#connect to the AWS socket
s = dsl.openSocketConn(host, port)

while True:
#	c, addr = s.accept()
    msg = s.recv(2048)
    
    #No database writing should happen if connection is interrupted
    if msg == None or msg == '':
        print "Check connection with server\n"
        dsl.closeSocketConn(s)
    else:
        print msg
        #dsl.sendMessageToGSM(numbers, msg)
    
        #The local ubuntu server is expected to receive a JSON message
        #parse the numbers from the message
        try:
            parsed_json = json.loads(msg)
            
            #print(parsed_json['numbers'])
            recipients = parsed_json['numbers']
            print "Recipients of Message: %s" % (len(recipients))
            
            for recipient in recipients:
                print recipient
            
            #print(parsed_json['msg'])
            message = parsed_json['msg']
            
            dsl.sendMessageToGSM(recipients, message)
        except:
            print "Error: Please check the JSON construction of your message"
        

        

#close AWS socket
dsl.closeSocketConn(s)




















