# -*- coding: utf-8 -*-
"""
Created on Tue Jun 28 15:18:27 2016

@author: PradoArturo
"""
import dewsSocketLib as dsl
from twisted.internet.protocol import ReconnectingClientFactory

from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory


if __name__ == '__main__':

    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    factory = dsl.MyClientFactory(u"ws://www.codesword.com:5050")

    reactor.connectTCP("www.codesword.com", 5050, factory)
    reactor.run()