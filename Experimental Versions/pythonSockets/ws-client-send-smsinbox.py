# -*- coding: utf-8 -*-
"""
Created on Wed Sep 23 13:59:18 2016

@author: PradoArturo
"""

import dewsSocketLeanLib as dsll

port = 5050
host = "www.dewslandslide.com"

dsll.sendBatchReceivedGSMtoDEWS(host, port, 5000)