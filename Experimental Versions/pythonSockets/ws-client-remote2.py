# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 13:59:18 2016

@author: PradoArturo
"""

from datetime import datetime
import dewsSocketLeanLib as dsll

#msg = "~`!@#$%^&*()_-+=qwertyuiop[]asdfghjkl;"
msg = "Are we back in business?"
curTS = datetime.now()
dsll.sendReceivedGSMtoDEWS(curTS, "09163677476", msg)