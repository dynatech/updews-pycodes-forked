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
# #import datetime
# from datetime import datetime
# import queryPiDb as qpi

# #Simple Python WebSocket
# from websocket import create_connection

#Autobahn and Twisted
from twisted.python import log
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory
from twisted.internet.protocol import ReconnectingClientFactory

import dewsSocketLeanLib as dsll

#import MySQLdb

#TODO: Add the accelerometer filter module you need to test
#import newAccelFilter as naf

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../Data Analysis'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs

class DewsClientGSMProtocol(WebSocketClientProtocol):
    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

        def ping():
            self.sendMessage(u"ping".encode('utf8'))
            self.factory.reactor.callLater(300, ping)

        # start sending messages every second ..
        ping()

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
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
                    
                    for recipient in recipients:
                        print recipient
                    
                    message = parsed_json['msg']
                    
                    dsll.sendMessageToGSM(recipients, message)
                    # self.sendMessage(u"Sent an SMS!".encode('utf8'))
                elif commType == 'smsrcv':
                    print "Warning: message type 'smsrcv', Message is ignored."
                else:
                    print "Error: No message type detected. Can't send an SMS."
                
            except:
                print "Error: Please check the JSON construction of your message"

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))

class DewsClientRegularProtocol(WebSocketClientProtocol):
    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
            msg = format(payload.decode('utf8'))
            print("Text message received: %s" % msg)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))

    def send(self, data):
        self.sendMessage(data)

class MyClientProtocol(WebSocketClientProtocol):

    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))
        self.factory.resetDelay()

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
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
                    
                    for recipient in recipients:
                        print recipient
                    
                    message = parsed_json['msg']
                    
                    dsll.sendMessageToGSM(recipients, message)
                    # self.sendMessage(u"Sent an SMS!".encode('utf8'))
                elif commType == 'smsrcv':
                    print "Warning: message type 'smsrcv', Message is ignored."
                else:
                    print "Error: No message type detected. Can't send an SMS."
                
            except:
                print "Error: Please check the JSON construction of your message"

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))


class MyClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

    protocol = MyClientProtocol

    def clientConnectionFailed(self, connector, reason):
        print("Client connection failed .. retrying ..")
        self.retry(connector)

    def clientConnectionLost(self, connector, reason):
        print("Client connection lost .. retrying ..")
        self.retry(connector)














