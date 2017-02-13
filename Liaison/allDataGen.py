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

        site = 'agb'
        fdate = '2016-01-01'
        tdate = '2016-04-04'
        df= vcd.vcdgen(site, tdate, fdate,1)

        print df
    
getDF();
