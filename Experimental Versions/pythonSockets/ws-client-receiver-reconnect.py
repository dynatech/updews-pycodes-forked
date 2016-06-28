# -*- coding: utf-8 -*-
"""
Created on Tue Jun 28 15:18:27 2016

@author: PradoArturo
"""
import dewsSocketLib as dsl
from twisted.internet.protocol import ReconnectingClientFactory

from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory


# class MyClientProtocol(WebSocketClientProtocol):

#     def onConnect(self, response):
#         print("Server connected: {0}".format(response.peer))
#         self.factory.resetDelay()

#     def onOpen(self):
#         print("WebSocket connection open.")

#     def onMessage(self, payload, isBinary):
#         if isBinary:
#             print("Binary message received: {0} bytes".format(len(payload)))
#         else:
#             msg = format(payload.decode('utf8'))
#             print("Text message received: %s" % msg)

#             #The local ubuntu server is expected to receive a JSON message
#             #parse the numbers from the message
#             try:
#                 parsed_json = json.loads(msg)
#                 commType = parsed_json['type']

#                 if commType == 'smssend':
#                     recipients = parsed_json['numbers']
#                     print "Recipients of Message: %s" % (len(recipients))
                    
#                     for recipient in recipients:
#                         print recipient
                    
#                     message = parsed_json['msg']
                    
#                     dsll.sendMessageToGSM(recipients, message)
#                     # self.sendMessage(u"Sent an SMS!".encode('utf8'))
#                 elif commType == 'smsrcv':
#                     print "Warning: message type 'smsrcv', Message is ignored."
#                 else:
#                     print "Error: No message type detected. Can't send an SMS."
                
#             except:
#                 print "Error: Please check the JSON construction of your message"

#     def onClose(self, wasClean, code, reason):
#         print("WebSocket connection closed: {0}".format(reason))


# class MyClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

#     protocol = MyClientProtocol

#     def clientConnectionFailed(self, connector, reason):
#         print("Client connection failed .. retrying ..")
#         self.retry(connector)

#     def clientConnectionLost(self, connector, reason):
#         print("Client connection lost .. retrying ..")
#         self.retry(connector)


if __name__ == '__main__':

    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    factory = dsl.MyClientFactory(u"ws://www.codesword.com:5050")

    reactor.connectTCP("www.codesword.com", 5050, factory)
    reactor.run()