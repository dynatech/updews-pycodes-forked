# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 13:52:25 2018

@author: Data Scientist 1
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np
import ConfigParser
from scipy.interpolate import UnivariateSpline
from scipy.signal import gaussian
from scipy.ndimage import filters
import os
import sys
import platform
from sqlalchemy import create_engine

def up_one(p):
    #INPUT: Path or directory
    #OUTPUT: Parent directory
    out = os.path.abspath(os.path.join(p, '..'))
    return out  


#Include the path of "Analysis/GroundAlert"folder for the python scrips searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
analysis_path = path+'/Analysis/GroundAlert//'
                      
if not analysis_path in sys.path:
    sys.path.insert(1,analysis_path)
del analysis_path

from GroundDataAlertLibWithTrendingNewAlerts import GenerateGroundDataAlert
from GroundDataAlertLibWithTrendingNewAlerts import PlotMarkerData
from GroundDataAlertLibWithTrendingNewAlerts import PlotTrendingAnalysis

plt.ion()

#### Main function
def main():
    '''
    Gets user input for site code and end timestamp, prints the corresponding marker alerts and shows all the relevant
    marker and trending plots.
    '''
    
    #### Get input and marker alerts data
    while True:
        site_code = raw_input('Input site code: ')
        end = raw_input('Input timestamp:')
        try:
            marker_alerts = GenerateGroundDataAlert(site_code,end)
            break
        except:
            print "Please check your input and try again."
    
    #### Generate marker plot
    PlotMarkerData(site_code,end)
    
    #### Check if marker alerts is not empty
    if len(marker_alerts) != 0:
    
        #### Generate trending plot if needed
        for marker in marker_alerts.loc[np.logical_and(marker_alerts.alert != 'l0',marker_alerts.alert != 'nd'),'marker_name'].values:
            PlotTrendingAnalysis(site_code,marker,end)
        
        #### Print marker alerts
        print "\n\nMarker alerts for site {}".format(site_code.upper())
        print marker_alerts[['marker_name','alert','displacement','time_delta']]

    else:
        #### Print no data
        print "\n\nNo data for site {} on timestamp {}".format(site_code.upper(),end)
    
#### Run main function
main()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    