# -*- coding: utf-8 -*-
"""
Created on Wed Jul 07 13:59:18 2016

@author: PradoArturo
"""

import dewsSocketLeanLib as dsll

port = 5050
host = "www.dewslandslide.com"
#host = "sandbox"

# Send ALL Acknowledgment messages of smsoutbox to the websocket server
# 	Messages included here have "send_status" values of:
# 	a. SENT
# 	b. FAIL
# 
# Once messages have been sent to the chatterbox server, the "send_status"
# 	will be updated to the following values:
# 	a. SENT -> SENT-WSS
#	b. FAIL -> FAIL-WSS
dsll.sendAllAckGSMToDEWS(host, port)