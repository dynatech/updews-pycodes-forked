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
    


site = 'magtb'
fdate = '2017-04-04 10:00:00'
tdate = '2017-04-05 10:00:00'

# site = sys.argv[1]
# fdate = sys.argv[2].replace("n",'').replace("T",' ')
# tdate = sys.argv[3].replace("n",'').replace("T",' ')
if fdate == '':
	df= vcd.vcdgen(site)
else:
	df= vcd.vcdgen(site, tdate, fdate)

print df

    

