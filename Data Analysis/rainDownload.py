import urllib
import urllib2
import ConfigParser
import math
from querySenslopeDb import *

cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')

#INPUT/OUTPUT FILES

#local file paths
ASTIpath = cfg.get('I/O', 'ASTIpath')
CSVFormat = cfg.get('I/O', 'CSVFormat')

## This will download CSV files that contain 14 days worth of data
#def getrain(site, gauge_num, rain_noah, offsetstart):
#    
#    if not math.isnan(rain_noah):
#        rain_noah = int(rain_noah)
#    
#    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=' + str(rain_noah)
#    print "downloading " + site + str(gauge_num)
#    urllib.urlretrieve(url, ASTIpath + site + ".csv")
    

    
def getrain(site, gauge_num, rain_noah, offsetstart):
    
    if not math.isnan(rain_noah):
        rain_noah = int(rain_noah)

    db, cur = SenslopeDBConnect(Namedb)
    
    query = "select timestamp,rval from senslopedb.rain_noah_%s" % str(rain_noah)
    query = query + " where timestamp >= timestamp('%s')" % offsetstart
    query = query + " order by timestamp desc"
    df =  GetDBDataFrame(query)
    df.columns = ['timestamp','rain']
    df.timestamp = pd.to_datetime(df.timestamp)
    df.to_csv(ASTIpath + site + CSVFormat, sep = ',', mode = 'w', index = False, header = False)
    