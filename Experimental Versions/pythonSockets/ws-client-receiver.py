import sys
import json
import dewsSocketLib as dsl
import queryPiDb as qpi

from twisted.python import log
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory

class DewsClientProtocol(WebSocketClientProtocol):

    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

        # def hello():
        #     self.sendMessage(u"Hello, world!".encode('utf8'))
        #     # self.sendMessage(b"\x00\x01\x03\x04", isBinary=True)
        #     self.factory.reactor.callLater(1, hello)

        # start sending messages every second ..
        # hello()

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
                
                recipients = parsed_json['numbers']
                print "Recipients of Message: %s" % (len(recipients))
                
                for recipient in recipients:
                    print recipient
                
                message = parsed_json['msg']
                
                dsl.sendMessageToGSM(recipients, message)
            except:
                print "Error: Please check the JSON construction of your message"

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))

def mainFunc(host, port):
    factory = WebSocketClientFactory(u"ws://%s:%s" % (host,port))
    factory.protocol = DewsClientProtocol

    reactor.connectTCP(host, port, factory)
    reactor.run()

if __name__ == '__main__':
    log.startLogging(sys.stdout)

    host = "www.codesword.com"
    port = 5050

    mainFunc(host, port)
