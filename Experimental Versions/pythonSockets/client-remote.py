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

##connect to the AWS socket
#s = dsl.openSocketConn(host, port)
#
#while True:
##	c, addr = s.accept()
#	msg = s.recv(1024)
#	print msg
#
##close AWS socket
#dsl.closeSocketConn(s)

dsl.sendDataFullCycle(host, port, """Testing this very very long message...
    The Quick Brown fox jumped over the lazy dog! Lets see how the server
    will parse it. Will it chop the message into pieces? Will the GSM be
    able to send it in proper chronological order?""")