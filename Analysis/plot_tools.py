
#end-of-event report plotting tools
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as pltdate
import numpy as np
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import seaborn as sns


from querySenslopeDb import *
from filterSensorData import *

def datetimetostr(petsa):
    return str(pltdate.num2date(petsa))

def datenum(petsa):
    #converts pandas datetime to integer used in plotting.
    petsa = petsa.to_pydatetime()
    petsa = pltdate.date2num(petsa)
    return petsa

def textTodatetime(text):
    t = pd.to_datetime(text)
    t = datenum(t)
    return t    
    
def accel_to_lin_xz_xy(seg_len,xa,ya,za):
    x=seg_len/np.sqrt(1+(np.tan(np.arctan(za/(np.sqrt(xa**2+ya**2))))**2+(np.tan(np.arctan(ya/(np.sqrt(xa**2+za**2))))**2)))
    xz=x*(za/(np.sqrt(xa**2+ya**2)))
    xy=x*(ya/(np.sqrt(xa**2+za**2)))
    
    return np.round(xz,4),np.round(xy,4)

def getSegLen(colname):
    query =  """SELECT seg_length from %s.site_column_props WHERE name = '%s' """ % (Namedb, colname)
    colprop = GetDBDataFrame(query)
    return colprop.seg_length[0]

def getGroundDF(site,start,end):

    query = 'SELECT timestamp,site_id,crack_id, meas FROM gndmeas '
    query += "where site_id = '%s' " % site
    query += "and timestamp <= '%s' "% end
    query += "and timestamp > '%s' " % start
    
    df = GetDBDataFrame(query)
    
    df['gts'] = df.timestamp.apply(datenum)
    return df

    
def getrain(rainsite,start,end):
    raindf = GetRawRainData(rainsite, fromTime=start, toTime=end)
    
    raindf = raindf.set_index('ts')
    raindf = raindf.resample('30min',how='sum')
    
    raindf['one_d'] = pd.rolling_sum(raindf.rain,48,min_periods=1)
    raindf['thr_d'] = pd.rolling_sum(raindf.rain,144,min_periods=1)
    
    raindf=raindf.reset_index()
    raindf['gts']  = raindf.ts.apply(datenum)     
    
    return raindf

def getSensor(start,end,colname,smooth=True):
    
    df = GetRawAccelData(siteid=colname, fromTime=start, toTime=end)
    seg_len = getSegLen(colname)    
    df = applyFilters(df)     
    
    df['xz'],df['xy']=accel_to_lin_xz_xy(seg_len,df.x.values,df.y.values,df.z.values)
    df=df.drop(['x','y','z'],axis=1)
    df = df.drop_duplicates(['ts', 'id'])
    
    df['gts'] = df.ts.apply(datenum)  
    
    df=df.set_index('ts')
 
    df=df[['id','xz','xy','gts']]
    
    return df

def plotNodes_xz(nodelist,grouped,ax):
    for node in nodelist:
        try:
            cur = grouped(node)
            ax.plot(cur.gts,cur.xz, label='Node '+str(node))
        except KeyError:
            pass
    ax.set_ylabel('Displacement (m)')
    ax.set_title('XZ Displacement for Selected Nodes')
    ax.legend(loc='best')
    return 0

def plotNodes_xy(nodelist,grouped,ax):
    for node in nodelist:
        try:
            cur = grouped(node)
            ax.plot(cur.gts,cur.xy, label='Node '+str(node))
        except KeyError:
            pass
    ax.set_ylabel('Displacement (m)')
    ax.set_title('XY Displacement for Selected Nodes')
    ax.legend(loc='best')
    return 0

def plotZeroedNodes_xy(nodelist,grouped,ax,name):
    for node in nodelist:
        try:
            cur = grouped(node)
            cur['disp'] = cur.xy-cur.xy.iloc[0]
            ax.plot(cur.gts,cur.disp, label='Node '+str(node),alpha=1)
        except KeyError:
            pass
    ax.set_ylabel('Change in \nDisplacement (m)')
    ax.set_title('Zeroed XZ Displacement for %s Nodes' % name)
    ax.legend(loc='left',ncol=2)
    return 0

def plotZeroedSmoothNodes_xz(nodelist,grouped,ax,name):
    for node in nodelist:
        try:
            cur = grouped(node)
            cur['xz'] = pd.rolling_mean(cur.xz,window=7,min_periods=1)[6:]
            cur = cur.dropna(axis=0)
            cur['disp'] = cur.xz-cur.xz.iloc[0]
            ax.plot(cur.gts,cur.disp, label='Node '+str(node),alpha=1)
        except KeyError:
            pass
    ax.set_ylabel('Change in \nDisplacement (m)')
    ax.set_title('Zeroed XZ Displacement for %s Nodes' % name)
    ax.legend(loc='best',ncol=8)
    return 0

def plotCml_xz(sens,ax,name):
    sens = smooth(sens)
    sens = sens.set_index('ts')
    timegroup = sens.reset_index().groupby('ts')
    
    dfts = timegroup[['xz']].sum()
    dfts = dfts.reset_index()
    dfts['gts'] = dfts.ts.apply(datenum)
    ax.plot(dfts.gts,dfts.xz)
    ax.set_ylabel('Cumulative \nDisplacement (m)')
    ax.set_title('Cumulative XZ Displacement for %s Nodes' % name)
    
    return 0

def plotZeroedNodes_xz(nodelist,grouped,ax,name):
    for node in nodelist:
        try:
            cur = grouped(node)
            cur['disp'] = cur.xz-cur.xz.iloc[0]
            ax.plot(cur.gts,cur.disp, label='Node '+str(node),alpha=1)
        except KeyError:
            pass
    ax.set_ylabel('Change in \nDisplacement (m)')
    ax.set_title('Zeroed XY Displacement for %s Nodes' % name)
    ax.legend(loc='best')
    return 0  

def plotSingleEvent(a,b,c,d,x,time,color,annotation):
    a.axvline(time,color=color,alpha=1)
    b.axvline(time,color=color,alpha=1)
    c.axvline(time,color=color,alpha=1)
    d.axvline(time,color=color,alpha=1)
    x.axvline(time,color=color,alpha=1)

    a.annotate(annotation,xy=(time,0.2),xytext=(time,0.2), fontsize='x-large')
    
    return 0


def plotRain(rain,rainsite,ax):
    ax.plot(rain.gts,rain.one_d, color='green', label='One Day',alpha=1)
    ax.plot(rain.gts,rain.thr_d, color='blue', label='Three day',alpha=1)
    ax2=ax.twinx()
    ax2.plot(rain.gts,rain.rain, marker='.',color='red', label = '15 Min',alpha=1)
    
    ax.set_ylim([0,rain.thr_d.max()*1.1])
    ax.set_title("%s Rainfall Data" % rainsite)  
    
    ax.set_ylabel('1D, 3D Rain (mm)')
    ax2.set_ylabel('15 Minute Rain (mm)')
    
    ax.set_yticks(np.linspace(ax.get_ybound()[0], ax.get_ybound()[1], 7))
    ax2.set_yticks(np.linspace(ax2.get_ybound()[0], ax2.get_ybound()[1], 7)) 
    ax2.grid(b=False)
    
    ax.legend(loc='upper right')
    ax2.legend(loc='lower right')
    return 0

def plotGround(ax,ground):
    ground['crack_id'] = map(lambda x: x.upper(), ground['crack_id'])
    grouped = ground.groupby('crack_id')
    
    for crack in ground.crack_id.unique():
        cur = grouped.get_group(crack)
        cur['disp'] = cur.meas-cur.meas.iloc[0]
        cur = cur.fillna(0)

        ax.plot(cur.gts,cur.disp, marker='o',label=crack.upper(),alpha=1)
        ax.set_ylabel('Change in \nMeasurement (cm)')
        ax.set_title('Ground Measurement Data')
        ax.legend(loc = 'best',ncol=3)
        
    return 0

def smooth(sens):
    smooth = sens.drop('gts')
    smooth = pd.rolling_mean(smooth,min_periods=1,window=7)[6:]
    smooth = smooth.reset_index()
    smooth['gts'] = smooth.ts.apply(datenum)
    
    return smooth

#############################
#user input
start = '2016-10-18 7:30:00'
end = '2016-10-18 17:30:00'

colname = 'tueta'
#rainsite = 'tuetaw'
groundsite = 'tue'
#############################
#get data
sens = getSensor(start,end,colname)
#nodelist = range(1,int(sens.id.max()))
nodelist = [4,11,13]

#new = pd.DataFrame()

#for node in nodelist:
#    new=new.append(sens[sens.id==node])

sens_group = sens.groupby('id').get_group
rain = getrain(rainsite,start,end)
ground = getGroundDF(groundsite,start,end)


#############################
# set up axes and plot
#cmap = sns.blend_palette(['yellow','red','green','blue','purple'],len(nodelist)+5)
f, (ax1, ax2, ax3,ax4) = plt.subplots(4, sharex=True, sharey=False)
ax4.xaxis_date()
ax4.xaxis.set_major_locator(pltdate.DayLocator(interval=10))
ax4.xaxis.set_major_formatter(pltdate.DateFormatter('%d-%b\n%a'))
ax4.xaxis.grid(True, which="major")

#############################
#actual plotting

plotCml_xz(sens,ax1,colname)
plotZeroedSmoothNodes_xz(nodelist,sens_group,ax2,colname)
plotGround(ax3,ground)
plotRain(rain,rainsite,ax4)


#############################




f.suptitle("Parasanon Event Timeline from %s to %s" % (start[:-3],end[:-3]),fontsize=14 )
plt.show()