# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 13:59:18 2016

@author: PradoArturo
"""

from websocket import create_connection
ws = create_connection("ws://www.codesword.com:5050")
print "Sending 'send_broadcast'..."
ws.send("send_broadcast:PAYLOAD")
print "Sent"
print "Receiving..."  # OPTIONAL
result = ws.recv()   # OPTIONAL
print "Received '%s'" % result    # OPTIONAL
ws.close()