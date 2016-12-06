import socket
import os
import sys
import thread
import time
import json
import simplejson
import pandas as pd
from datetime import datetime

#Simple Python WebSocket
from websocket import create_connection

import basicDB as bdb
import common
import masynckaiserServerRequests as masyncSR
import masynckaiserGetData as masyncGD

# Initiate a database table creation on the web socket server using the 
# special client request
def pushTableCreation(ws=None, schema=None, table=None):
    if not ws or not schema or not table:
        print "%s ERROR: No ws|schema|table value passed" % (common.whoami())
        return false

    qShowCreateTable = "SHOW CREATE TABLE %s" % (table)
    qTableCreation = (bdb.GetDBResultset(qShowCreateTable, schema))[0][1]
    # print qTableCreation
    requestMsg = masyncSR.modifierQuery(schema, qTableCreation)
    # print requestMsg

    if requestMsg:    
        ws.send(requestMsg)
        result = ws.recv()
        # print "Result: %s" % (result)

        if result == "false":
            print "Table (%s) creation on Web Server Failed" % (table)
            return False
        elif result == "true":
            print "Table (%s) creation on Web Server SUCCEEDED!" % (table)
            return True