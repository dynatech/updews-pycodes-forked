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


import querySenslopeDb as q
import configfileio as cfg

config = cfg.config()

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

#def getEQ():    
#    query = """ SELECT * FROM %s.earthquake order by timestamp desc limit 1 """ % (q.Namedb)
#    dfeq =  q.GetDBDataFrame(query)
#    return dfeq.mag[0],dfeq.lat[0],dfeq.longi[0],dfeq.timestamp[0]

def getUnprocessed():
    query = """ select * from %s.earthquake where processed=0 """ % (q.Namedb)
    dfeq =  q.GetDBDataFrame(query)
#    dfeq = dfeq.set_index('e_id')
    return dfeq

def getSites():
    query = """ SELECT * FROM %s.site_column """ % (q.Namedb)
    df = q.GetDBDataFrame(query)
    return df[['name','lat','lon']]

def uptoDB(df):
    engine=create_engine('mysql://root:senslope@192.168.150.129:3306/senslopedb')
    df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = True)
    
def uptoSandBox(df):
    engine=create_engine('mysql://root:senslope@192.168.150.129:3306/senslopedb')
    df.to_sql(name = 'earthquake_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)

def execQuery(query):
        db, cur = q.SenslopeDBConnect(q.Namedb)
        cur.execute(query)
        db.commit()
        db.close()


    
output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

if not os.path.exists(output_path+config.io.outputfilepath+'EQ/'):
    os.makedirs(output_path+config.io.outputfilepath+'EQ/')

output_path += config.io.outputfilepath+'EQ/'
table_name = 'earthquake'

eq_a = pd.DataFrame(columns=['site_id','eq_id','distance'])
######################################################MAIN

dfeq = getUnprocessed()

dfeq.set_index('e_id', inplace=True)

to_plot = 1

for i in dfeq.index:
    cur = dfeq.loc[i]
    
    mag, eq_lat, eq_lon,ts = cur.mag, cur.lat, cur.longi,cur.timestamp
       
    
    critdist = getCritDist(mag)

    if False in np.isfinite([mag,eq_lat,eq_lon]): #has NaN value in mag, lat, or lon 
        query = """ update %s set processed = -1 where e_id = %s """ % (table_name, i)
        execQuery(query)
        continue
     
    
    print mag
    if mag >=4:    
        sites = getSites()
        sites['site'] = sites['name'].str[:3]
        sites = sites.drop_duplicates('site')
        sites = sites.drop('name',1)
        dfg = sites.groupby('site')
        sites = dfg.apply(getrowDistancetoEQ)
        
        query = """update %s set processed = 1, critdist = %s where e_id = %s""" % (table_name,critdist,i)
        execQuery(query) 
        
        sites = sites[sites.lat>1]
        
        
        crits = sites[sites.dist<=critdist]
        
        if len(crits.site.values) > 0:
    
        
            crits['timestamp']  = ts
            crits['source'] = 'eq'
            crits['alert'] = 'e1'
            crits['updateTS'] = ts
            eq_a = crits            
            crits = crits[['timestamp','site','source','alert','updateTS']].set_index('timestamp')
            
            eq_a = eq_a[['site','dist']]
            eq_a = eq_a.rename(columns={'site':'site_id','dist':'distance'})
            eq_a['eq_id'] = i
            eq_a = eq_a[['site_id','eq_id','distance']]
            
            try:
                uptoDB(crits)
                
                
            except:
                pass
            uptoSandBox(eq_a)
            query = """ update %s set processed = 1, critdist = %s where e_id = %s """ % (table_name,critdist,i)
            execQuery(query)            
            
            if to_plot:
                plotsites = sites[sites.dist <= critdist*3]
                plotsites = plotsites.append([{'lat':eq_lat,'lon':eq_lon,'site':''}])
                
                lats = pd.Series.tolist(plotsites.lat)
                lons = pd.Series.tolist(plotsites.lon)
                labels = pd.Series.tolist(plotsites.site)
                
                
                plotcrits = pd.merge(crits, plotsites, on='site')
                critlats = pd.Series.tolist(plotcrits.lat)
                critlons = pd.Series.tolist(plotcrits.lon)
                
                critlons.append(critlons[-1])
                critlats.append(critlats[-1])
                
                critlabels = pd.Series.tolist(plotcrits.site)
                
                
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
            print "> No affected sites. "
            query = """ update %s set processed = 1 where e_id = %s """ % (table_name,i)
            execQuery(query)       
    
    else:
        print '> Magnitude too small. '
        query = """ update %s set processed = 1 where e_id = %s """ % (table_name,i)
        execQuery(query)  
        pass

print datetime.now()