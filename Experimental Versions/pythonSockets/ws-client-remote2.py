# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 13:59:18 2016

@author: PradoArturo
"""

from datetime import datetime
import dewsSocketLeanLib as dsll

port = 5051

#msg = "~`!@#$%^&*()_-+=qwertyuiop[]asdfghjkl;"
#msg = "Are we back in business?"
#msg = """codesword recommendations (Jul-15 09:31): BHI 5.68% ALT 5.26% ZHI 4.92% MED 3.51% UNI 3.23% WEB 3.03% CEI 3.01% IS 2.74% MWIDE 1.96% DD 1.9% CPG 1.82% BC 1.52% BRN 1.52% EEI 1.51% ARA 1.3%"""
msg = 'Seems like it is still working properly!'
curTS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#dsll.sendReceivedGSMtoDEWS(curTS, "09165555555", msg)
#dsll.sendReceivedGSMtoDEWS(curTS, "09167777777", msg)
#dsll.sendReceivedGSMtoDEWS(curTS, "09168888888", msg)
#dsll.sendReceivedGSMtoDEWS(curTS, "09169999999", msg, port)
#dsll.sendReceivedGSMtoDEWS(curTS, "09980619501", msg)
#dsll.sendReceivedGSMtoDEWS(curTS, "09163677476", msg)