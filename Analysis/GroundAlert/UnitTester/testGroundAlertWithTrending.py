# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 18:08:15 2016

@author: PradoArturo
"""

import os
import sys
import time
from datetime import datetime
import pandas as pd

#include the path of "Ground Alert" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path    

#import Ground Data Alert Library with Trending
import GroundDataAlertLibWithTrending as test

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path    

#import Data Analysis/querySenslopeDb
import querySenslopeDb as qs

# Test basic inputs
#output = test.GroundDataTrendingPlotJSON("agb", "A", end = datetime.now())
#output = test.GroundDataTrendingPlotJSON("lab", "D", end = datetime.now())

try:
    # Get list of 3-letter site code
    querySiteCodes = """
                    SELECT DISTINCT LEFT(name, 3) as name 
                    FROM site_column
                    ORDER BY name"""
    siteCodes = qs.GetDBDataFrame(querySiteCodes)
    
    for site in siteCodes["name"]:
        print site
        # Get list of distinct crack ids per site
        queryCrackIDs = """
                        SELECT DISTINCT crack_id 
                        FROM gndmeas 
                        WHERE site_id = "%s" 
                        ORDER BY crack_id        
                        """ % (site)
        crackIDs = qs.GetDBDataFrame(queryCrackIDs)
        
        for cid in crackIDs["crack_id"]:
            print cid
            output = test.GroundDataTrendingPlotJSON(site, cid, end = datetime.now())
            print "\n"
            
        print "\n\n"
        
except IndexError:
    print '>> Error in writing extracting database data to files..'

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    