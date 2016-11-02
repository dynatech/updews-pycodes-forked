# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 18:08:15 2016

@author: PradoArturo
"""

import os
import sys
import time
import pandas as pd
#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path    

#import Data Analysis/querySenslopeDb
import querySenslopeDb as qs

#import Velocity, Column Position and Displacement Generator Library
import vcdgen as vcd

#vcd.displacement("mamb")

fdate = "2016-05-01 00:00:00"
#tdate = time.strftime("%Y-%m-%d %H:%M")
tdate = "2016-06-09 15:00:00"
fixpoints = ['top', 'bottom', 'TOP', 'BOTTOM', 'Top', 'Bottom','']

try:
    #Get list of sensors
    sensorsInfo = qs.GetSensorDF()
    columns = sensorsInfo["name"]
    
#    for fixpoint in fixpoints:
#        print fixpoint
    
    for column in columns:
        print """Current Sensor Column: %s, End Date: %s, Start Date: %s""" % (column, tdate, fdate)
        vcdout = vcd.vcdgen(column, tdate, fdate, 1)
        print vcdout        
        
except IndexError:
    print '>> Error in writing extracting database data to files..'
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    