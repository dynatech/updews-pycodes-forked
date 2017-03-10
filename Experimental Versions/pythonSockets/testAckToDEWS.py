# -*- coding: utf-8 -*-
"""
Created on Wed Jul 07 13:59:18 2016

@author: PradoArturo
"""

import dewsSocketLeanLib as dsll

port = 5050
# host = "www.dewslandslide.com"
host = "sandbox"

# Send 100 messages from smsinbox to the websocket server at a time
dsll.sendAllAckGSMToDEWS(host, port)