import urllib
import urllib2
import ConfigParser
import math
import os
import sys

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

from querySenslopeDb import *

cfg = ConfigParser.ConfigParser()
cfg.read('IO-Config.txt')

#INPUT/OUTPUT FILES

#local file paths
ASTIpath = output_path + cfg.get('I/O', 'ASTIpath')
CSVFormat = cfg.get('I/O', 'CSVFormat')

#To Output File or not
PrintASTIdata = cfg.getboolean('I/O','PrintASTIdata')

if PrintASTIdata:
    if not os.path.exists(ASTIpath):
        os.makedirs(ASTIpath)

## This will download CSV files that contain 13 days worth of data   
def getrain(site, gauge_num, rain_noah, offsetstart):

    try:    
        if not math.isnan(rain_noah):
            rain_noah = int(rain_noah)
    
        db, cur = SenslopeDBConnect(Namedb)
        
        query = "select timestamp,rval from senslopedb.rain_noah_%s" % str(rain_noah)
        query = query + " where timestamp >= timestamp('%s')" % offsetstart
        query = query + " order by timestamp desc"
        df =  GetDBDataFrame(query)
        df.columns = ['timestamp','rain']
        df.timestamp = pd.to_datetime(df.timestamp)
        if PrintASTIdata:
            df.to_csv(ASTIpath + site + CSVFormat, sep = ',', mode = 'w', index = False, header = False)
        df.set_index('timestamp', inplace = True)
        return df
    except:
        print 'Table senslopedb.rain_noah_' + str(rain_noah) + " doesn't exist"
        df = pd.DataFrame(data=None)
        return df