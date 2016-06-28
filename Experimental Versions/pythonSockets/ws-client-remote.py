import sys
import json
import time
import dewsSocketLib as dsl

from twisted.python import log
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory

# def wsClientReceiver(host, port):
#     factory = WebSocketClientFactory(u"ws://%s:%s" % (host,port))
#     factory.protocol = dsl.DewsClientRegularProtocol

#     reactor.connectTCP(host, port, factory)
#     factory.protocol.sendMessage(factory.protocol, "Hello Fuckers!")
#     reactor.run()

# if __name__ == '__main__':
#     log.startLogging(sys.stdout)

#     host = "www.codesword.com"
#     port = 5050

#     wsClientReceiver(host, port)

class WebSocketProtocol(WebSocketClientProtocol):
    def __init__(self):
        self.instance = []

    def onOpen(self):
        # self.factory.instance = []
        self.instance.append(self)

    def retInstance(self):
        return self

websocket_factory = WebSocketClientFactory("ws://www.codesword.com:5050")
websocket_factory.protocol = WebSocketProtocol
reactor.connectTCP("www.codesword.com", 5050, websocket_factory)
reactor.callFromThread(WebSocketClientProtocol.sendMessage, websocket_factory.protocol.instance[0], send)