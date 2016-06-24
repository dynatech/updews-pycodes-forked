# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 13:59:18 2016

@author: PradoArturo
"""

from datetime import datetime
import dewsSocketLeanLib as dsll

msg = "~`!@#$%^&*()_-+=qwertyuiop[]asdfghjkl;"
curTS = datetime.now()
dsll.sendReceivedGSMtoDEWS(curTS, "09163677476", msg)