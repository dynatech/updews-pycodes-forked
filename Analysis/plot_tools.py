
#end-of-event report plotting tools
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as pltdate
import numpy as np
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
#import seaborn as sns
plt.ion()


import querySenslopeDb as q
import filterSensorData as f

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
    query =  """SELECT seg_length from %s.site_column_props WHERE name = '%s' """ % (q.Namedb, colname)
    colprop = q.GetDBDataFrame(query)
    return colprop.seg_length[0]

def getGroundDF(site,start,end):

    query = 'SELECT timestamp,site_id,crack_id, meas FROM gndmeas '
    query += "where site_id = '%s' " % site
    query += "and timestamp <= '%s' "% end
    query += "and timestamp > '%s' " % start
    
    df = q.GetDBDataFrame(query)
    
    df['gts'] = df.timestamp.apply(datenum)
    return df

    
def getrain(rainsite,start,end):
    raindf = q.GetRawRainData(rainsite, fromTime=start, toTime=end)
    
    raindf = raindf.set_index('ts')
    raindf = raindf.resample('30min',how='sum')
    
    raindf['one_d'] = pd.rolling_sum(raindf.rain,48,min_periods=1)
    raindf['thr_d'] = pd.rolling_sum(raindf.rain,144,min_periods=1)
    
    raindf=raindf.reset_index()
    raindf['gts']  = raindf.ts.apply(datenum)     
    
    return raindf

def getSensor(start,end,colname,smooth=True):
    
    df = q.GetRawAccelData(siteid=colname, fromTime=start, toTime=end)
    seg_len = getSegLen(colname)    
    df = f.applyFilters(df)     
    
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
            cur['xy'] = pd.rolling_mean(cur.xy,window=7,min_periods=1)[6:]
            cur = cur.dropna(axis=0)
            cur['xy'] = cur['xy'].apply(lambda x: x * 10)
            cur['disp'] = cur.xy-cur.xy.iloc[0]
            ax.plot(cur.gts,cur.disp, label=name + ' ' +str(node),alpha=1)
        except KeyError:
            pass
    ax.set_ylabel('Change in \nDisplacement (cm)')
    ax.set_title('Zeroed XY Displacement for %s Nodes' % name[0:3])
    ax.legend(loc='left',ncol=2, fontsize='small')
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
            cur['xz'] = pd.rolling_mean(cur.xz,window=7,min_periods=1)[6:]
            cur = cur.dropna(axis=0)
            cur['xz'] = cur['xz'].apply(lambda x: x * 10)
            cur['disp'] = cur.xz-cur.xz.iloc[0]
            ax.plot(cur.gts,cur.disp, label=name + ' ' +str(node),alpha=1)
        except KeyError:
            pass
    ax.set_ylabel('Change in \nDisplacement (cm)')
    ax.set_title('Zeroed XZ Displacement for %s Nodes' % name)
    ax.legend(loc='best', fontsize='small', ncol=3)
    return 0  

def plotSingleEvent(lst,time,color,annotation):
    for i in lst:
        i.axvline(time,color=color,alpha=1)

    lst[0].annotate(annotation,xy=(time,-25),xytext=(time,-25), fontsize='x-large')
    
    return 0


def plotRain(rain,rainsite,ax):
    ax.plot(rain.gts,rain.one_d, color='green', label='1-day cml',alpha=1)
    ax.plot(rain.gts,rain.thr_d, color='blue', label='3-day cml',alpha=1)
#    ax2=ax.twinx()
#    ax2.plot(rain.gts,rain.rain, marker='.',color='red', label = '15 Min',alpha=1)
    
    query = "SELECT * FROM rain_props where name = '%s'" %rainsite[0:3]
    df = q.GetDBDataFrame(query)
    twoyrmax = df['max_rain_2year'].values[0]
    halfmax = twoyrmax/2
    
#    ax.axhline(halfmax,color='green',alpha=1)
#    ax.axhline(twoyrmax,color='blue',alpha=1)
    ax.plot(rain.gts, [halfmax]*len(rain.gts), color='green', label='1/2 of 2-yr max', alpha=1, linestyle='--')
    ax.plot(rain.gts, [twoyrmax]*len(rain.gts), color='blue', label='2-yr max', alpha=1, linestyle='--')
    
    ax.set_ylim([0,rain.thr_d.max()*1.1])
    ax.set_title("%s Rainfall Data" % rainsite)  
    
    ax.set_ylabel('1D, 3D Rain (mm)')
#    ax2.set_ylabel('15 Minute Rain (mm)')
    
    ax.set_yticks(np.linspace(ax.get_ybound()[0], twoyrmax+5, 7))
#    ax2.set_yticks(np.linspace(ax2.get_ybound()[0], ax2.get_ybound()[1], 7)) 
#    ax2.grid(b=False)
    
    ax.legend(loc='upper left', fontsize='small')
#    ax2.legend(loc='lower right')
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
        ax.set_yticks(np.linspace(ax.get_ybound()[0]-3.5, ax.get_ybound()[1]+3.5, 7))
        ax.set_title('Ground Measurement Data')
        ax.legend(loc = 'best',ncol=3, fontsize='small')
        
    return 0

def smooth(sens):
    smooth = sens.drop('gts')
    smooth = pd.rolling_mean(smooth,min_periods=1,window=7)[6:]
    smooth = smooth.reset_index()
    smooth['gts'] = smooth.ts.apply(datenum)
    
    return smooth

############################# IMMULI JULY 31 TO AUG 5 EVENT ##################################
#
#
##############################
##user input
#start = pd.to_datetime('2016-07-29 00:00:00')
#end = pd.to_datetime('2016-08-06 00:00:00')
#
#colname1 = 'imuta'
#colname2 = 'imusc'
#rainsite = 'imuw'
#groundsite = 'imu'
##############################
##get data
#sens1 = getSensor(start,end,colname1)
##nodelist = range(1,int(sens.id.max()))
#nodelist1 = [16,22]
#sens2 = getSensor(start,end,colname2)
##nodelist = range(1,int(sens.id.max()))
#nodelist2 = [6,7]
#
#
#
##new = pd.DataFrame()
#
##for node in nodelist:
##    new=new.append(sens[sens.id==node])
#
#sens1_group = sens1.groupby('id').get_group
#sens2_group = sens2.groupby('id').get_group
#rain = getrain(rainsite,start,end)
#ground = getGroundDF(groundsite,start,end)
#print ground
#
#
##############################
## set up axes and plot
##cmap = sns.blend_palette(['yellow','red','green','blue','purple'],len(nodelist)+5)
#fig=plt.figure(figsize = (40,50))
#ax1 = fig.add_subplot(311)
#ax2 = fig.add_subplot(312, sharex=ax1)
#ax3 = fig.add_subplot(313, sharex=ax1)
##ax4 = fig.add_subplot(414, sharex=ax1)
#
##############################
##actual plotting
#
##plotCml_xz(sens,ax1,colname)
#plotZeroedNodes_xz(nodelist1,sens1_group,ax1,colname1)
##plotZeroedNodes_xz(nodelist2,sens2_group,ax1,colname2)
#
##plotZeroedNodes_xy(nodelist1,sens1_group,ax2,colname1)
##plotZeroedNodes_xy(nodelist2,sens2_group,ax2,colname2)
#
#plotGround(ax2,ground)
#plotRain(rain,rainsite,ax3)
#
#plotSingleEvent([ax1,ax2,ax3],pd.to_datetime('2016-08-01 00:00'),'red','L2')
#plotSingleEvent([ax1,ax2,ax3],pd.to_datetime('2016-07-31 17:00'),'red','r1')
#plotSingleEvent([ax1,ax2,ax3],pd.to_datetime('2016-08-01 07:00'),'red','l2')
#
#ax1.grid(True)
#ax2.grid(True)
#ax3.grid(True)
##ax4.grid(True)
#
#ax1.xaxis_date()
#ax1.xaxis.set_major_locator(pltdate.DayLocator(interval=1))
#ax1.xaxis.set_major_formatter(pltdate.DateFormatter('%b %d'))
#
##############################
#
#fig.subplots_adjust(top=0.85, hspace=0.35)
#fig.suptitle("Immuli Event Timeline from %s to %s" % (start.date(),end.date()),fontsize=20)
#plt.savefig('D:\Documents\DYNA\NIGSCON2016\\'+groundsite+' event '+str(start.date())+' to '+str(end.date())+'.png',
#                dpi=400, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
#
############################################################################################


############################# IMMULI AUG 15 TO AUG 27 EVENT ##################################
#
#
##############################
##user input
#start = pd.to_datetime('2016-08-14 00:00:00')
#end = pd.to_datetime('2016-08-28 00:00:00')
#
#colname1 = 'imuta'
#colname2 = 'imutb'
#colname3 = 'imusc'
#rainsite = 'imuw'
#groundsite = 'imu'
##############################
##get data
#sens1 = getSensor(start,end,colname1)
##nodelist = range(1,int(sens.id.max()))
#nodelist1 = range(2, 25)
#sens2 = getSensor(start,end,colname2)
##nodelist = range(1,int(sens.id.max()))
#nodelist2 = [1,2,3,4,9,10,11,13,14]
#sens3 = getSensor(start,end,colname3)
##nodelist = range(1,int(sens.id.max()))
#nodelist3 = range(2,6) + range(7,16)
#
#
##new = pd.DataFrame()
#
##for node in nodelist:
##    new=new.append(sens[sens.id==node])
#
#sens1_group = sens1.groupby('id').get_group
#sens2_group = sens2.groupby('id').get_group
#sens3_group = sens3.groupby('id').get_group
#rain = getrain(rainsite,start,end)
#ground = getGroundDF(groundsite,start,end)
#
##############################
## set up axes and plot
##cmap = sns.blend_palette(['yellow','red','green','blue','purple'],len(nodelist)+5)
#fig=plt.figure()
#ax1 = fig.add_subplot(511)
#ax2 = fig.add_subplot(512, sharex=ax1)
#ax3 = fig.add_subplot(513, sharex=ax1)
#ax4 = fig.add_subplot(514, sharex=ax1)
#ax5 = fig.add_subplot(515, sharex=ax1)
#
##############################
##actual plotting
#
##plotCml_xz(sens,ax1,colname)
#plotZeroedNodes_xz(nodelist1,sens1_group,ax1,colname1)
#plotZeroedNodes_xz(nodelist2,sens2_group,ax2,colname2)
#plotZeroedNodes_xz(nodelist3,sens3_group,ax3,colname3)
#
##plotZeroedNodes_xy(nodelist1,sens1_group,ax2,colname1)
##plotZeroedNodes_xy(nodelist2,sens2_group,ax2,colname2)
#
#plotGround(ax4,ground)
#plotRain(rain,rainsite,ax5)
#
#plotSingleEvent([ax1,ax2,ax3, ax4, ax5],pd.to_datetime('2016-08-15 06:00'),'red','r1')
#plotSingleEvent([ax1,ax2,ax3, ax4, ax5],pd.to_datetime('2016-08-15 07:00'),'red','l2')
#plotSingleEvent([ax1,ax2,ax3, ax4, ax5],pd.to_datetime('2016-08-16 08:30'),'red','L2')
#plotSingleEvent([ax1,ax2,ax3, ax4, ax5],pd.to_datetime('2016-08-17 01:30'),'red','L3')
#plotSingleEvent([ax1,ax2,ax3, ax4, ax5],pd.to_datetime('2016-08-17 13:00'),'red','l3')
#
#
#ax1.grid(True)
#ax2.grid(True)
#ax3.grid(True)
#ax4.grid(True)
#ax5.grid(True)
#
#ax1.xaxis_date()
#ax1.xaxis.set_major_locator(pltdate.DayLocator(interval=2))
#ax1.xaxis.set_major_formatter(pltdate.DateFormatter('%b %d'))
#
##############################
#
#fig.subplots_adjust(top=0.85, hspace=0.4)
#fig.set_figheight(16)
#fig.set_figwidth(11)
#fig.suptitle("Immuli Event Timeline from %s to %s" % (start.date(),end.date()),fontsize=20)
#plt.savefig('D:\Documents\DYNA\NIGSCON2016\\'+groundsite+' event '+str(start.date())+' to '+str(end.date())+'.png',
#                dpi=400, facecolor='w', edgecolor='w',orientation='portrait',mode='w')
#
############################################################################################

############################ PARASANON SEP 16 TO SEP 21 EVENT ##################################


#############################
#user input
start = pd.to_datetime('2016-09-14 00:00:00')
end = pd.to_datetime('2016-11-12 00:00:00')

rainsite = 'partaw'
groundsite = 'par'
#############################
#get data

rain = getrain(rainsite,start,end)
ground = getGroundDF(groundsite,start,end)



#############################
# set up axes and plot
#cmap = sns.blend_palette(['yellow','red','green','blue','purple'],len(nodelist)+5)
fig=plt.figure()
ax1 = fig.add_subplot(211)
ax2 = fig.add_subplot(212, sharex=ax1)

#############################
#actual plotting

plotGround(ax1,ground)
plotRain(rain,rainsite,ax2)

plotSingleEvent([ax1,ax2],pd.to_datetime('2016-09-16 09:58'),'red','l2')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-09-27 07:05'),'red','l3')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-14 09:24'),'red','l2')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-21 11:05'),'red','l2')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-25 11:50'),'red','l2')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-30 11:28'),'red','l2')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-11-04 08:46'),'red','l3')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-11-04 19:00'),'red','r1')

plotSingleEvent([ax1,ax2],pd.to_datetime('2016-09-21 12:00'),'blue','A0')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-11 12:00'),'blue','A0')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-17 16:00'),'blue','A0')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-22 16:00'),'blue','A0')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-10-28 12:00'),'blue','A0')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-11-02 16:00'),'blue','A0')
plotSingleEvent([ax1,ax2],pd.to_datetime('2016-11-11 08:00'),'blue','A0')

ax1.grid(True)
ax2.grid(True)

ax1.xaxis_date()
ax1.xaxis.set_major_locator(pltdate.DayLocator(interval=5))
ax1.xaxis.set_major_formatter(pltdate.DateFormatter('%b %d'))

#############################

fig.subplots_adjust(top=0.85, hspace=0.4)
fig.set_figheight(10)
fig.set_figwidth(15)
fig.suptitle("Parasanon Event Timeline from %s to %s" % (start.date(),end.date()),fontsize=20)
plt.savefig('D:\Documents\DYNA\NIGSCON2016\\'+groundsite+' event '+str(start.date())+' to '+str(end.date())+'.png',
                dpi=400, facecolor='w', edgecolor='w',orientation='portrait',mode='w')

###########################################################################################