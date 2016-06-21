import sys
import json
import dewsSocketLib as dsl

from twisted.python import log
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory

def wsClientReceiver(host, port):
    factory = WebSocketClientFactory(u"ws://%s:%s" % (host,port))
    # factory.protocol = DewsClientProtocol
    factory.protocol = dsl.DewsClientGSMProtocol

    reactor.connectTCP(host, port, factory)
    reactor.run()

if __name__ == '__main__':
    log.startLogging(sys.stdout)

    host = "www.codesword.com"
    port = 5050

    wsClientReceiver(host, port)
