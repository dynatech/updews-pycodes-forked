# -*- coding: utf-8 -*-
"""
Created on Wed Jul 07 13:59:18 2016

@author: PradoArturo
"""

import dewsSocketLeanLib as dsll

port = 5050
# host = "www.dewslandslide.com"
host = "sandbox"

# Send ALL messages from smsinbox to the websocket server
# 	messages with web_flag = 'W' and read_status = 'READ-SUCCESS'
# 	Other inbox messages are ignored
dsll.sendAllSmsInboxToDEWS(host, port)