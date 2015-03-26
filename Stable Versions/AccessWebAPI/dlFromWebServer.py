import urllib
import urllib2

'''
function dlFromWebServer: downloads data from webserver
  startDate: (string)
    start date of data to be downloaded
    yyyy-mm-dd
  site: (string)
    corresponding column name of data
  db: (string)
    database name from webserver to download data from
    
  sample usage:
    import dlFromWebServer from dlFromWebServer as dl
    dl('2015-03-01','sinb','senslopedb')
'''

def dlFromWebServer(startDate,site,db):

  url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=' + db + '&accelsite&site=' + site + '&q=' + startDate
  print "downloading " + site
  print url
  
  try:
    a = urllib.urlopen(url)
    data = a.read().strip()
  except:
    raise ValueError('Unexpected error')
    
  if "ERROR" in data:
    raise ValueError(data)
  elif "Failed to connect to MySQL" in data:
    raise SystemError(data)
  else:
    return data
