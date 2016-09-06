# -*- coding: utf-8 -*-
"""
Created on Wed Jul 07 13:59:18 2016

@author: PradoArturo
"""

from websocket import create_connection
import time
import dewsSocketLeanLib as dsll

url = "www.dewslandslide.com"
port = 5050
dsll.connRecvReconn(url, port)