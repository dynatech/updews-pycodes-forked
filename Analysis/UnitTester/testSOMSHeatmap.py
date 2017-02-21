# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 10:00:00 2017

@author: Prado Arturo Bognot
"""

from datetime import datetime
import os
import pandas as pd
import random
import sys
import time


#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path    

#import Data Analysis/querySenslopeDb
import querySenslopeDb as qs

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../Soms'))
if not path in sys.path:
    sys.path.insert(1,path)
del path    

#import Heatmap Library
import heatmap as hmap

# test = hmap.heatmap(col="agbsb", t_timestamp=tdate, t_win = '1d')

# Date Randomizer
def dateRandomizer():
	year = random.randint(2013, 2017)
	month = random.randint(1, 12)
	day = random.randint(1, 28)
	hour = random.randint(0, 23)
	minute = random.randint(0, 59)
	ts_rand = datetime(year, month, day, hour, minute).strftime("%Y-%m-%d %H:%M")
	
	return ts_rand

# Test Block for trying different iterations on the soms heatmap function
def testBlock(_tdate = None, _twin = None):
	if (_tdate == None) or (_twin == None):
		print "Error: No target date or time window input"
		return

	try:
	    #Get list of sensors
	    sensorsInfo = qs.GetSensorDF()
	    columns = sensorsInfo["name"]  
	    
	    for column in columns:
	        print """\n\nCurrent Sensor Column: %s, Target Date: %s, Time Window: %s \n\n""" % (column, _tdate, _twin)
	        hmap_data = hmap.heatmap(col=column, t_timestamp=_tdate)
	        print hmap_data        
	        
	except IndexError:
	    print '>> Error in writing extracting database data to files..'

# Time Window Options
time_window = ['1d', '3d', '30d']
for t_win in time_window:
	### Current Date Testing
	tdate = time.strftime("%Y-%m-%d %H:%M")
	testBlock(dateRandomizer(), t_win)

	### Random Date Testing
	testBlock(dateRandomizer(), t_win)

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    