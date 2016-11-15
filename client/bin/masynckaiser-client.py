# -*- coding: utf-8 -*-
"""
Created on Mon Oct 03 17:44:15 2016

@author: PradoArturo
"""

from datetime import datetime
import threading
import os
import sys
import time
import json
import pandas as pd

#include the path of "libraries" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../libraries'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import masynckaiserWSSLib as masync
import basicDB as bdb

def wssSendMsg(msgid = 1, number = "09980619501"):
	threading.Timer(1.0, wssSendMsg).start()

	port = 5060

	#msg = "~`!@#$%^&*()_-+=qwertyuiop[]asdfghjkl;"
	# msg = "Are we back in business?"
	#msg = """codesword recommendations (Jul-15 09:31): BHI 5.68% ALT 5.26% ZHI 4.92% MED 3.51% UNI 3.23% WEB 3.03% CEI 3.01% IS 2.74% MWIDE 1.96% DD 1.9% CPG 1.82% BC 1.52% BRN 1.52% EEI 1.51% ARA 1.3%"""
	curTS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	msg = '(%s) Seems like it is still working properly!: %s' % (msgid, curTS)
	#masync.sendReceivedGSMtoDEWS(curTS, "09169999999", msg, port)
	masync.sendReceivedGSMtoDEWS(curTS, "09980619501", msg)

def printit():
	threading.Timer(1.0, printit).start()
	print "Hello, World!"

def main():
    #printit()
    # wssSendMsg()
    host = "sandbox"
    port = 5055
    schema = "senslopedb"
    table = "smsoutbox"
    
    masync.syncStartUp(host, port)
#    masync.syncRealTime(host, port)
#    masync.testSendRecv(host, port, schema, table)
#    masync.threadedSendRecv(host, port, schema, table)

#main()

host = "sandbox"
port = 5055
schema = "senslopedb"
table = "smsoutbox"

output = masync.syncStartUp(host, port, 1000)
# bdb.PushDBjson(jsonData=output, table_name='smsinbox', schema_name='senslopedb', insType='ignore')

#msg = format(schemas.decode('utf8'))
#parsed_json = json.loads(json.loads(msg))

#schemaList = []
#for json_dict in parsed_json:
#    for key,value in json_dict.iteritems():
#        print("key: {} | value: {}".format(key, value))
#        schemaList.append(value)


