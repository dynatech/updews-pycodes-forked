## SOEPD parser + earthquake alert gen
# outputs eqsummary.txt - a summary of affected sites wrt to an earthquake event 
# equation used is from first graph of fig 2 of "On far field occurence of seismically induced landslides"
# equation of upper bound curve derived using G3Data

import urllib
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import querySenslopeDb as q
import ConfigParser

# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()

cfg.read(configFile)

#local file paths
output_file_path = cfg.get('I/O','OutputFilePath')

#file names
eqsummary = cfg.get('I/O', 'eqsummary')

#Set True for JSON printing
set_json = True


dataset =[None]*6
end = datetime.now().replace(microsecond=0)
end

#Get the sensor list and initialize the JSON df container
alert_df = {}
sensorlist = q.GetSensorList()
for s in sensorlist:
    alert_df.update({s.name:'e0'})


try:
    #url = 'not a url'    
    url = 'http://www.phivolcs.dost.gov.ph/html/update_SOEPD/EQLatest.html'
    html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html,'html.parser')
    
    #cleaning text input from phivolcs-soepd earthquake database
    table = soup.find('table', {'class':'MsoNormalTable', 'border':'0'})
    table = table.findNext('table') #first table is "seismicity maps"
    header = table.find('tr', {'style':'mso-yfti-irow:0;mso-yfti-firstrow:yes;height:36.75pt'})
    row = header.findNext('tr')#first <tr> is table headers

    x=0
    for td in row.findChildren('td'):  
        dataset[x]=td.text
        dataset[x]=dataset[x].replace(u'\xa0', u' ')
        dataset[x]=dataset[x].replace(u'\xb0', u'')
        dataset[x]=dataset[x].replace(u'\xba', u'')
        dataset[x]=dataset[x].replace(u'\r\n','\n')
        dataset[x]=dataset[x].replace(u'\n','')
        dataset[x]=dataset[x].replace(u'\t',' ')
        dataset[x]=dataset[x].encode('utf8')
        
        if x==0:
            dataset[x]=dataset[x].replace(u' ', '')
            ts=datetime.strptime(dataset[x],'%d%b%Y-%I:%M%p')
            
        elif x==1:
            dataset[x]=dataset[x].replace(u' ', '')
            lat = float(dataset[x])
            
        elif x==2:
            dataset[x]=dataset[x].replace(u' ', '')
            lon = float(dataset[x])
            
        elif x==3:
            dataset[x]=dataset[x].replace(u' ', '')
            dep = float(dataset[x])
            
        elif x==4:
            dataset[x]=dataset[x].replace(u' ', '')
            mag= float(dataset[x])
            
        elif x==5:
            rel=dataset[x]
        x+=1
    
#    uncomment if testing a user-specified quake
#    mag= 6.7
#    lat= 07.17
#    lon= 125.54
#    ts = end-timedelta(minutes=15)
    
    with open (output_file_path+eqsummary, 'w') as z:
        z.write (('as of ') + str(end) + ':\n')
        
    #checks if quake is within last 30mins   
        if ts > (end-timedelta(minutes=30)):
            
            if mag>=4:
                    critdist= (29.027 * (mag**2)) - (251.89*mag) + 547.97
                    z.write( 'magnitude ' + str(mag) + ' earthquake at ' + str(lat) + 'N ' + str(lon) + 'E' + ' on ' + str(ts) + '\n')
                    z.write('critical distance at ' + str(critdist) + ' km' + '\n')
                    z.write("start monitoring for ff sites:" + '\n')
                    cnt = 0
                    
                    sensors = q.GetCoordsList()                    
                    coords = pd.DataFrame(columns=['name','lat','lon'])

                    for s in sensors:
                        coords.loc[s] = pd.Series({'name':s.name, 'lat':s.lat, 'lon':s.lon})
                        
                    for s in range(len(sensors)):
                    #slon, slat -> site long and lat
                    #dlon, dlat -> difference between site lon-lat and epicenter lon-lat
                    #formula for distance is Haversine formula
                       colname, slon, slat = coords['name'][s], coords['lon'][s],coords['lat'][s]     
                       dlon=lon-slon
                       dlat=lat-slat
                       dlon=np.radians(dlon)
                       dlat=np.radians(dlat)
                       a=(np.sin(dlat/2))**2 + ( np.cos(np.radians(lat)) * np.cos(np.radians(slat)) * (np.sin(dlon/2))**2 )
                       c= 2 * np.arctan2(np.sqrt(a),np.sqrt(1-a))
                       d= 6371 * c
                                      
                       if d <= critdist:
                           z.write( colname + ': E1' + '\n')
                           alert_df.update({colname:'e1'})
                           cnt+=1
                           
                    if cnt==0: 
                        z.write( 'all sites E0' + '\n')
                
            elif mag<4:
                z.write('all sites E0')
                    
        else:
           z.write('all sites E0')
#           z.write('-last earthquake out of time range. last earthquake was at ' + str(ts)+', ' + rel)
           
except IOError:
    end = datetime.now().replace(microsecond=0)
    with open (output_file_path+eqsummary, 'w') as z:
        z.write (('as of ') + str(end) + ':\n')
        z.write('Error. Please check if SOEPD site is down or your internet connection. \n')
        z.write('SOEPD site: http://www.phivolcs.dost.gov.ph/html/update_SOEPD/EQLatest.html')
        
print 'eqsummary done'

#Printinf of JSON Format
if set_json:
    #create data frame as for easy conversion to JSON format
    alert_df = sorted(alert_df.items())
    for i in range(len(alert_df)): alert_df[i] = (end,) + alert_df[i]
    
    dfa = pd.DataFrame(alert_df,columns = ['timestamp','site','eq alert'])
    
    #converting the data frame to JSON format
    dfajson = dfa.to_json(orient="records",date_format='iso')
    
    #ensuring proper datetime format
    i = 0
    while i <= len(dfajson):
        if dfajson[i:i+9] == 'timestamp':
            dfajson = dfajson[:i] + dfajson[i:i+36].replace("T"," ").replace("Z","").replace(".000","") + dfajson[i+36:]
            i += 1
        else:
            i += 1
    print dfajson


    