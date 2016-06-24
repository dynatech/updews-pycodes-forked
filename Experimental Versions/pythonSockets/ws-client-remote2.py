# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 13:59:18 2016

@author: PradoArturo
"""

import dewsSocketLeanLib as dsll

msg = "~`!@#$%^&*()_-+=qwertyuiop[]asdfghjkl;"

dsll.sendReceivedGSMtoDEWS("2016-06-22 17:00:00", "09163677476", msg)