##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()


import numpy as np
import pandas as pd
import sys
from datetime import datetime
from sqlalchemy import create_engine
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import os


from querySenslopeDb import *



sys.path.insert(0, '/home/dynaslope/Desktop/Senslope Server')


def plotBasemap(ax,eq_lat,eq_lon,plotsites,critdist):
    latmin = plotsites.lat.min()-0.2
    latmax = plotsites.lat.max()+0.2
    lonmin = plotsites.lon.min()-0.2
    lonmax = plotsites.lon.max()+0.2
    
    m = Basemap(llcrnrlon=lonmin,llcrnrlat=latmin,urcrnrlon=lonmax,urcrnrlat=latmax,
            resolution='f',projection='merc',lon_0=(lonmin+lonmax)/2,lat_0=
            (latmin+latmax)/2            
            )
            
    m.drawcoastlines()
    m.fillcontinents(color='coral',lake_color='aqua')
    
    # draw parallels and meridians.
    del_lat = latmax - latmin
    del_lon = lonmax - lonmin
    m.drawparallels(np.arange(latmin,latmax,del_lat/4),labels=[True,True,False,False])
    merids=m.drawmeridians(np.arange(lonmin,lonmax,del_lon/4),labels=[False,False,False,True])
    
    for x in merids:
        try:
            merids[x][1][0].set_rotation(10)
        except:
            pass
        
    m.drawmapboundary(fill_color='aqua')
    plt.title("Earthquake Map for Event\n Mag %s, %sN, %sE" % (str(mag),str(eq_lat),str(eq_lon)))
    
    return m,ax
    
def plotEQ(m,mag,eq_lat,eq_lon,ax):
    critdist = getCritDist(mag)    
    x,y = m(eq_lon, eq_lat)
    m.scatter(x,y,c='red',marker='o',zorder=10,label='earthquake')
    m.tissot(eq_lon,eq_lat,get_radius(critdist),256,zorder=5,color='red',alpha=0.4)

def get_radius(km):
    return float(np.rad2deg(km/6371.))    

def getCritDist(mag):
    return (29.027 * (mag**2)) - (251.89*mag) + 547.97

def getrowDistancetoEQ(df):#,eq_lat,eq_lon):   
    dlon=eq_lon-df.lon
    dlat=eq_lat-df.lat
    dlon=np.radians(dlon)
    dlat=np.radians(dlat)
    a=(np.sin(dlat/2))**2 + ( np.cos(np.radians(eq_lat)) * np.cos(np.radians(df.lat)) * (np.sin(dlon/2))**2 )
    c= 2 * np.arctan2(np.sqrt(a),np.sqrt(1-a))
    d= 6371 * c
    
    df['dist'] = d    
    
    return df

def getEQ():    
    query = """ SELECT * FROM %s.earthquake order by timestamp desc limit 1 """ % (Namedb)
    dfeq =  GetDBDataFrame(query)
    return dfeq.mag[0],dfeq.lat[0],dfeq.longi[0],dfeq.timestamp[0]

def getSites():
    query = """ SELECT * FROM %s.site_column """ % (Namedb)
    df = GetDBDataFrame(query)
    return df[['name','lat','lon']]

def uptoDB(df):
    engine=create_engine('mysql://root:senslope@192.168.150.129:3306/senslopedb')
    df.to_sql(name = 'earthquake_alerts', con = engine, if_exists = 'append', schema = Namedb, index = True)

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

if not os.path.exists(output_path+config.io.outputfilepath+'EQ/'):
    os.makedirs(output_path+config.io.outputfilepath+'EQ/')

#MAIN

mag,eq_lat,eq_lon,ts = 5.3,11.00, 122.10,datetime.now()

#mag,eq_lat,eq_lon,ts = getEQ()

#print getEQ()

critdist = getCritDist(mag)

print mag
if mag >=4:    
    sites = getSites()
    sites['name'] = sites['name'].str[:3]
    sites = sites.drop_duplicates('name')
#    sites = sites.drop('name',1)
    dfg = sites.groupby('name')
    sites = dfg.apply(getrowDistancetoEQ)
        
    
    sites = sites[sites.lat>1]
    
    
    crits = sites[sites.dist<critdist]
#    crits = crits.reset_index()

    if len(crits.name.values) > 0:
#        message = "EQALERT\nAs of %s: \nE1: %s" % (str(ts),','.join(str(n) for n in crits.sitename.values))
#        print message
#        WriteEQAlertMessageToDb(message)
    
        crits['timestamp']  = ts
        crits['alert'] = 'E1'
        crits = crits[['timestamp','name','alert']].set_index('timestamp')
    
        uptoDB(crits)
    
        plotsites = sites[sites.dist <= critdist*3]
        plotsites = plotsites.append([{'lat':eq_lat,'lon':eq_lon,'name':''}])
        
        lats = pd.Series.tolist(plotsites.lat)
        lons = pd.Series.tolist(plotsites.lon)
        labels = pd.Series.tolist(plotsites.name)
        
        
        plotcrits = pd.merge(crits, plotsites, on='name')
        critlats = pd.Series.tolist(plotcrits.lat)
        critlons = pd.Series.tolist(plotcrits.lon)
        
        critlons.append(critlons[-1])
        critlats.append(critlats[-1])
        
        critlabels = pd.Series.tolist(plotcrits.name)
        
        
        fig,ax = plt.subplots(1)
        m,ax=plotBasemap(ax,eq_lat,eq_lon,plotsites,critdist)
        plotEQ(m,mag,eq_lat,eq_lon,ax)
        
        try:
            m.plot(lons,lats,label='sites',marker='o',latlon=True,linewidth=0,color='yellow')
        
        except IndexError: #basemap has error when plotting exactly one or two items, duplicate an item to avoid
            lons.append(lons[-1])
            lats.append(lats[-1])
            m.plot(lons,lats,label='sites',marker='o',latlon=True,linewidth=0,color='yellow')
        
        try:
            m.plot(critlons,critlats,latlon=True,label='critical sites',markersize=12,linewidth=1, marker='^', color='red')
        
        except IndexError: #basemap has error when plotting exactly two items. duplicate last entry
            critlons.append(critlons[-1])
            critlats.append(critlats[-1])
            m.plot(critlons,critlats,latlon=True,label='critical sites',markersize=12,linewidth=1, marker='^', color='red')
        
        
        x,y = m(lons,lats)
        
        for n in range(len(plotsites)):
            try:
                ax.annotate(labels[n],xy=(x[n],y[n]),fontweight='bold',fontsize=12)
            except IndexError:
                pass
            
        plt.savefig(output_path+'eq_%s' % ts.strftime("%Y-%m-%d %H-%M-%S"))
    
    else:
        print "> No affected sites."

else:
    print '> Magnitude too small.'
    pass

