import os
import sys
import time
from datetime import datetime
import pandas as pd

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../updews-pycodes/Analysis/'))
if not path in sys.path:
   sys.path.insert(1, path)
del path

import vcdgen as vcd
# import querySenslopeDb as qs
    
def getDF():
    
    #site_column = sys.argv[1]
    #tdate = sys.argv[2].replace("n",'').replace("T"," ").replace("%20"," ")
    #fdate = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")

    site_column = "agbta"
    tdate = "2017-11-11 06:00:00"
    fdate = "2017-11-08 06:00:00"
    
    df= vcd.vcdgen(site_column, tdate, fdate)
    
    print df

getDF();
