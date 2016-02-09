import urllib
import urllib2
import ConfigParser
import math

cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')

#INPUT/OUTPUT FILES

#local file paths
ASTIpath = cfg.get('I/O', 'ASTIpath')

## This will download CSV files that contain 14 days worth of data
def getrain(site, gauge_num, rain_noah):
    
    if not math.isnan(rain_noah):
        rain_noah = int(rain_noah)
    
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=' + str(rain_noah)
    print "downloading " + site + str(gauge_num)
    urllib.urlretrieve(url, ASTIpath + site + ".csv")