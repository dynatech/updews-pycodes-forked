import os
import sys
import pandas as pd
import numpy as np
#import outputFilteredData as ofd
from datetime import timedelta as td
from datetime import datetime as dt
from sqlalchemy import create_engine

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs
import outputFilteredData as ofd

def GenLsbAlerts():
    sites = qs.GetSensorList()
    
    alertTxt = ""
    alertTxt2 = ""
    print "Getting lsb alerts"
    
    for site in sites:
        for nid in range(1, site.nos+1):
            df = ofd.getFilteredData(isCmd = False, inSite = site.name, inNode = nid, inStart = (dt.now()-td(7)).strftime("%y/%m/%d %H:%M:%S"))
            isDFempty = df.empty
            
            if isDFempty == True:
                PrintOut('No Data Available... for %s %s' % (site.name, nid))    
                continue
            
            df = df.set_index(['ts'])

            df2 = df.copy()
            dfa = []

            try:
                df3 = df2.resample('30Min').fillna(method='pad')
            except pd.core.groupby.DataError:
                #print "No data to resample %s %s" % (site.name, nid)
                continue
            dfv = df3 - df3.shift(12) 

            if len(dfa) == 0:
                dfa = dfv.copy()
            else:
                dfa = dfa.append(dfv)

            window = 48
            dfarm = pd.rolling_mean(dfa, window)
            dfarm = dfarm[dfarm.index > dt.now()-td(1)]
            if (((abs(dfarm.x)>0.25) | (abs(dfarm.y)>0.25) | (abs(dfarm.z)>1.0)).any()):
                ins = "%s,%s" % (site.name, nid)
                alertTxt += ins
                alertTxt2 += ins
                print ins + '\t',
                
                if ((abs(dfarm.x)>0.25).any()):
                    print 'x',
                    alertTxt += ',1'
                    alertTxt2 += ',' + repr(max(abs(dfarm.x)))
                else:
                    alertTxt += ',0'
                    alertTxt2 += ',0'

                if ((abs(dfarm.y)>0.25).any()):
                    print 'y',
                    alertTxt += ',1'
                    alertTxt2 += ',' + repr(max(abs(dfarm.y)))
                else:
                    alertTxt += ',0'
                    alertTxt2 += ',0'
                
                if ((abs(dfarm.z)>1.0).any()):
                    print 'z',
                    alertTxt += ',1'
                    alertTxt2 += ',' + repr(max(abs(dfarm.z)))
                else:
                    alertTxt += ',0'
                    alertTxt2 += ',0'

                print ''
                alertTxt += '\n'
                alertTxt2 += '\n'
            
    f = open('lsbalerts.csv', 'w')
    f.write(alertTxt)
    f.close()
    
    f = open('lsbalerts2.csv', 'w')
    f.write(alertTxt2)
    f.close()

def main():
    
    GenLsbAlerts()
        
    
if __name__ == '__main__':
    main()
