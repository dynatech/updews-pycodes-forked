##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ion()

import os
import pandas as pd
import numpy as np
from datetime import timedelta

import rtwindow as rtw
import querySenslopeDb as q
import genproc as g
import errorAnalysis as err
from scipy.interpolate import UnivariateSpline
from scipy.signal import gaussian
from scipy.ndimage import filters

def fukuzono_constants(v_min = False,v_max = False,plot = False,numpts = 1000):
    ##### Constants from Federico et al. 2011 data
    if v_max == False:
        v_max = 1788.20274072984
    if v_min == False:
        v_min = 0.0474997943466147
    v_n = np.linspace(v_min,v_max,numpts)
    
    slope = 1.49905955613175
    intercept = -3.00263765777028
    t_crit = 4.53047399738543
    var_v_log = 215.515369339559
    v_log_mean = 2.232839766
    sum_res_square = 49.8880017417971
    n = 30.
    
    uncertainty = t_crit*np.sqrt(1/(n-2)*sum_res_square*(1/n + (np.log(v_n) - v_log_mean)**2/var_v_log))
    
    a_theo_log = slope*np.log(v_n) + intercept
    a_theo_log_up = a_theo_log + uncertainty
    a_theo_log_down = a_theo_log - uncertainty
    
    a_n = np.e**a_theo_log
    a_n_up = np.e**a_theo_log_up
    a_n_down = np.e**a_theo_log_down
    
    return a_n, a_n_up, a_n_down

def moving_average(series,sigma = 3):
    b = gaussian(39,sigma)
    average = filters.convolve1d(series,b/b.sum())
    var = filters.convolve1d(np.power(series-average,2),b/b.sum())
    return average,var

def goodness_of_fit(x,y,reg):
    mean = np.mean(y)
    n = float(len(y))
    SS_tot = np.sum(np.power(y-mean,2))
    SS_res = np.sum(np.power(y-reg,2))
    coef_determination = 1 - SS_res/SS_tot
    RMSE = np.sqrt(SS_res/n)
    return SS_res,coef_determination,RMSE    

def col_pos(colpos_dfts, col_pos_end, col_pos_interval, col_pos_number, num_nodes):
    
    colpos_dfts = colpos_dfts.drop_duplicates()
    cumsum_df = colpos_dfts[['xz','xy']].cumsum()
    colpos_dfts['cs_xz'] = cumsum_df.xz.values
    colpos_dfts['cs_xy'] = cumsum_df.xy.values
    
    return np.round(colpos_dfts, 4)
                
def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_color_cycle([cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in range(NUM_COLORS)])
    return ax
    
    
def subplot_colpos(dfts, ax_xz, ax_xy, show_part_legend, i):
    #current column position x
    curcolpos_x = dfts.x.values

    #current column position xz
    curax = ax_xz
    curcolpos_xz = dfts.cs_xz.values
    curax.plot(curcolpos_xz,curcolpos_x,'.-')
    curax.set_xlabel('xz')
    curax.set_ylabel('x')
    
    #current column position xy
    curax=ax_xy
    curcolpos_xy = dfts.cs_xy.values
    if show_part_legend == False:
        curax.plot(curcolpos_xy,curcolpos_x,'.-', label=str(pd.to_datetime(dfts.ts.values[0])))
    else:
        if i % show_part_legend == 0 or i == config.io.num_col_pos:
            curax.plot(curcolpos_xy,curcolpos_x,'.-', label=str(pd.to_datetime(dfts.ts.values[0])))
        else:
            curax.plot(curcolpos_xy,curcolpos_x,'.-')
    curax.set_xlabel('xy')
    return
    
    
def plot_column_positions(df,colname,end, show_part_legend):
#==============================================================================
# 
#     DESCRIPTION
#     returns plot of xz and xy absolute displacements of each node
# 
#     INPUT
#     colname; array; list of sites
#     x; dataframe; vertical displacements
#     xz; dataframe; horizontal linear displacements along the planes defined by xa-za
#     xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
#==============================================================================

    try:
        fig=plt.figure()
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)
        
        ax_xz=nonrepeat_colors(ax_xz,len(set(df.ts.values)))
        ax_xy=nonrepeat_colors(ax_xy,len(set(df.ts.values)))

        colposTS = sorted(set(df.ts), reverse = False)

        for i in range(len(set(df.ts))):
            subplot_colpos(df.loc[df.ts == colposTS[i]], ax_xz=ax_xz, ax_xy=ax_xy, show_part_legend=show_part_legend, i=i)

        for tick in ax_xz.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
            
        for tick in ax_xy.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
       
        for tick in ax_xz.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
            
        for tick in ax_xy.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)

        plt.tight_layout()
        plt.subplots_adjust(top=0.945)        
        plt.suptitle(colname+" as of "+str(end),fontsize='medium')

    except:        
        print colname, "ERROR in plotting column position"
    return ax_xz,ax_xy

def vel_plot(df, velplot):
    velplot[df.id.values[0]] = df.id.values[0]
    return velplot

def vel_classify(df, config):
    vel=pd.DataFrame(index=sorted(set(df.ts)))
    nodal_df = df.groupby('id')
    velplot = nodal_df.apply(vel_plot, velplot=vel)
    velplot = velplot.reset_index().loc[velplot.reset_index().id == len(set(df.id))][['level_1'] + range(1, len(set(df.id))+1)].rename(columns = {'level_1': 'ts'}).set_index('ts')
    df = df.set_index(['ts', 'id'])
    try:
        L2mask = (df.abs()>config.io.t_vell2)&(df.abs()<=config.io.t_vell3)        
        L3mask = (df.abs()>config.io.t_vell3)
        L2mask = L2mask.reset_index().replace(False, np.nan)
        L3mask = L3mask.reset_index().replace(False, np.nan)
                    
        return velplot,L2mask,L3mask
    except:
        print "ERROR computing velocity classification"
        return 
    
def noise_envelope(df, tsdf):
    df['ts'] = tsdf
    return df

def plot_disp_vel(df, colname, max_min_df, window, config, plotvel, disp_offset = 'mean'):
#==============================================================================
# 
#     DESCRIPTION:
#     returns plot of xz and xy displacements per node, xz and xy velocities per node
# 
#     INPUT:
#     xz; array of floats; horizontal linear displacements along the planes defined by xa-za
#     xy; array of floats; horizontal linear displacements along the planes defined by xa-ya
#     xz_vel; array of floats; velocity along the planes defined by xa-za
#     xy_vel; array of floats; velocity along the planes defined by xa-ya
#==============================================================================

    num_nodes = len(set(df.id))
    df = df.loc[(df.ts >= window.start)&(df.ts <= window.end)]
  
    #setting up zeroing and offseting parameters
    nodal_df = df.groupby('id')
    if disp_offset == 'max':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).max()
    elif disp_offset == 'mean':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).mean()
    elif disp_offset == 'min':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).min()
    else:
        xzd_plotoffset = 0


    # defining cumulative (surface) displacement
    dfts = df.groupby('ts')
    cs_df = dfts[['xz', 'xy']].sum()    
    cs_df = cs_df - cs_df.values[0] + xzd_plotoffset * num_nodes
    cs_df = cs_df.sort_index()
    
    #creating noise envelope
    first_row = df.loc[df.ts == window.start].sort_values('id').set_index('id')[['xz', 'xy']]
        
    max_min_df['xz_maxlist'] = max_min_df['xz_maxlist'].values - first_row['xz'].values
    max_min_df['xz_minlist'] = max_min_df['xz_minlist'].values - first_row['xz'].values
    max_min_df['xy_maxlist'] = max_min_df['xy_maxlist'].values - first_row['xy'].values
    max_min_df['xy_minlist'] = max_min_df['xy_minlist'].values - first_row['xy'].values
        
    max_min_df = max_min_df.reset_index()
    max_min_df = max_min_df.append([max_min_df] * (len(set(df.ts))-1), ignore_index = True)
    nodal_max_min_df = max_min_df.groupby('id')

    noise_df = nodal_max_min_df.apply(noise_envelope, tsdf = sorted(set(df.ts)))
    nodal_noise_df = noise_df.groupby('id')
    noise_df = nodal_noise_df.apply(df_add_offset_col, offset = xzd_plotoffset, num_nodes = num_nodes)
    noise_df = noise_df.set_index('ts')

    # conpensates double offset of node 1 due to df.apply
    a = noise_df.loc[noise_df.id == 1] - (num_nodes - 1) * xzd_plotoffset
    a['id'] = 1
    noise_df = noise_df.loc[noise_df.id != 1]
    noise_df = noise_df.append(a)
    noise_df = noise_df.sort_index()
    
    nodal_noise_df = noise_df.groupby('id')
    
    #zeroing and offseting xz,xy
    df0 = nodal_df.apply(df_zero_initial_row, window = window)
    nodal_df0 = df0.groupby('id')
    df0off = nodal_df0.apply(df_add_offset_col, offset = xzd_plotoffset, num_nodes = num_nodes)
    df0off = df0off.set_index('ts')
    
    # conpensates double offset of node 1 due to df.apply
    a = df0off.loc[df0off.id == 1] - (num_nodes - 1) * xzd_plotoffset
    a['id'] = 1
    df0off = df0off.loc[df0off.id != 1]
    df0off = df0off.append(a)
    df0off = df0off.sort_index()
    
    nodal_df0off = df0off.groupby('id')
    
#    try:
    fig=plt.figure()
    
    if plotvel:
        #creating subplots        
        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
        
        ax_xzv=fig.add_subplot(143)
        ax_xzv.invert_yaxis()
        ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)
    else:
        #creating subplots        
        ax_xzd=fig.add_subplot(121)
        ax_xyd=fig.add_subplot(122,sharex=ax_xzd,sharey=ax_xzd)            
    
    #plotting cumulative (surface) displacments
    ax_xzd.plot(cs_df.index, cs_df['xz'].values,color='0.4',linewidth=0.5)
    ax_xyd.plot(cs_df.index, cs_df['xy'].values,color='0.4',linewidth=0.5)
    ax_xzd.fill_between(cs_df.index,cs_df['xz'].values,xzd_plotoffset*(num_nodes),color='0.8')
    ax_xyd.fill_between(cs_df.index,cs_df['xy'].values,xzd_plotoffset*(num_nodes),color='0.8')       
   
    #assigning non-repeating colors to subplots axis
    ax_xzd=nonrepeat_colors(ax_xzd,num_nodes)
    ax_xyd=nonrepeat_colors(ax_xyd,num_nodes)
    if plotvel:
        ax_xzv=nonrepeat_colors(ax_xzv,num_nodes)
        ax_xyv=nonrepeat_colors(ax_xyv,num_nodes)

    #plotting displacement for xz
    curax=ax_xzd
    plt.sca(curax)
    nodal_df0off['xz'].apply(plt.plot)
    nodal_noise_df['xz_maxlist'].apply(plt.plot, ls=':')
    nodal_noise_df['xz_minlist'].apply(plt.plot, ls=':')
    curax.set_title('displacement\n XZ axis',fontsize='small')
    curax.set_ylabel('displacement scale, m', fontsize='small')
    y = df0off.loc[df0off.index == window.start].sort_values('id')['xz'].values
    x = window.start
    z = sorted(set(df.id))
    for i,j in zip(y,z):
       curax.annotate(str(int(j)),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')
    
    #plotting displacement for xy
    curax=ax_xyd
    plt.sca(curax)
    nodal_df0off['xy'].apply(plt.plot)
    nodal_noise_df['xy_maxlist'].apply(plt.plot, ls=':')
    nodal_noise_df['xy_minlist'].apply(plt.plot, ls=':')
    curax.set_title('displacement\n XY axis',fontsize='small')
    y = df0off.loc[df0off.index == window.start].sort_values('id')['xy'].values
    x = window.start
    z = sorted(set(df.id))
    for i,j in zip(y,z):
       curax.annotate(str(int(j)),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')

    if plotvel:
        #plotting velocity for xz
        curax=ax_xzv
        plt.sca(curax)
        vel_xz = df[['ts', 'vel_xz', 'id']]
        vel_xz = vel_xz.loc[(vel_xz.ts >= window.end - timedelta(hours=3)) & (vel_xz.ts <= window.end)]
        velplot,L2,L3 = vel_classify(vel_xz, config)  
        velplot.plot(ax=curax,marker='.',legend=False)

        L2 = L2.sort_values('ts', ascending = True).set_index('ts')
        nodal_L2 = L2.groupby('id')
        nodal_L2['vel_xz'].apply(plt.plot,marker='^',ms=8,mfc='y',lw=0,)

        L3 = L3.sort_values('ts', ascending = True).set_index('ts')
        nodal_L3 = L3.groupby('id')
        nodal_L3['vel_xz'].apply(plt.plot,marker='^',ms=10,mfc='r',lw=0,)
        
        y = sorted(set(df.id))
        x = window.end - timedelta(hours=2.5)
        z = sorted(set(df.id))
        for i,j in zip(y,z):
            curax.annotate(str(int(j)),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')            
        curax.set_ylabel('node ID', fontsize='small')
        curax.set_title('velocity alerts\n XZ axis',fontsize='small')  
    
        #plotting velocity for xy        
        curax=ax_xyv
        plt.sca(curax)   
        vel_xy = df[['ts', 'vel_xy', 'id']]
        vel_xy = vel_xy.loc[(vel_xy.ts >= window.end - timedelta(hours=3)) & (vel_xy.ts <= window.end)]
        velplot,L2,L3 = vel_classify(vel_xy, config)
        velplot.plot(ax=curax,marker='.',legend=False)
        
        L2 = L2.sort_values('ts', ascending = True).set_index('ts')
        nodal_L2 = L2.groupby('id')
        nodal_L2['vel_xy'].apply(plt.plot,marker='^',ms=8,mfc='y',lw=0,)

        L3 = L3.sort_values('ts', ascending = True).set_index('ts')
        nodal_L3 = L3.groupby('id')
        nodal_L3['vel_xy'].apply(plt.plot,marker='^',ms=10,mfc='r',lw=0,)
               
        y = sorted(set(df.id))
        x = window.end - timedelta(hours=2.5)
        z = sorted(set(df.id))
        for i,j in zip(y,z):
            curax.annotate(str(int(j)),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')            
        curax.set_title('velocity alerts\n XY axis',fontsize='small')                        
        
    # rotating xlabel
    
    for tick in ax_xzd.xaxis.get_minor_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)
        
    for tick in ax_xyd.xaxis.get_minor_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)
    
    for tick in ax_xzd.xaxis.get_major_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)
        
    for tick in ax_xyd.xaxis.get_major_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)

    if plotvel:
        for tick in ax_xzv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
    
        for tick in ax_xyv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
            
        for tick in ax_xzv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
    
        for tick in ax_xyv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
            
    for item in ([ax_xzd.xaxis.label, ax_xyd.xaxis.label]):
        item.set_fontsize(8)

    if plotvel:
        for item in ([ax_xyv.yaxis.label, ax_xzv.yaxis.label]):
            item.set_fontsize(8)
        
    dfmt = md.DateFormatter('%Y-%m-%d\n%H:%M')
    ax_xzd.xaxis.set_major_formatter(dfmt)
    ax_xyd.xaxis.set_major_formatter(dfmt)
        
    fig.tight_layout()
    
    fig.subplots_adjust(top=0.85)        
    fig.suptitle(colname+" as of "+str(window.end),fontsize='medium')
    line=mpl.lines.Line2D((0.5,0.5),(0.1,0.8))
    fig.lines=line,
        
#    except:      
#        print colname, "ERROR in plotting displacements and velocities"
    return


def df_zero_initial_row(df, window):
    #zeroing time series to initial value;
    #essentially, this subtracts the value of the first row
    #from all the rows of the dataframe
    columns = list(df.columns)
    columns.remove('ts')
    columns.remove('id')
    for m in columns:
        df[m] = df[m] - df.loc[df.ts == window.start][m].values[0]
    return np.round(df,4)

def df_add_offset_col(df, offset, num_nodes):
    #adding offset value based on column value (node ID);
    #topmost node (node 1) has largest offset
    columns = list(df.columns)
    columns.remove('ts')
    columns.remove('id')
    for m in columns:
        df[m] = df[m] + (num_nodes - df.id.values[0]) * offset
    return np.round(df,4)
    
    
def main(monitoring, window, config, plotvel=True, show_part_legend = False):

    colname = monitoring.colprops.name
    num_nodes = monitoring.colprops.nos
    seg_len = monitoring.colprops.seglen
    monitoring_vel = monitoring.vel.reset_index()[['ts', 'id', 'xz', 'xy', 'vel_xz', 'vel_xy']]
    
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    
    if not os.path.exists(output_path+config.io.outputfilepath+'realtime/'):
        os.makedirs(output_path+config.io.outputfilepath+'realtime/')

    # noise envelope
    max_min_df, max_min_cml = err.cml_noise_profiling(monitoring_vel)
        
    # compute column position
    colposdates = pd.date_range(end=window.end, freq=config.io.col_pos_interval, periods=config.io.num_col_pos, name='ts', closed=None)
    colpos_df = pd.DataFrame({'ts': colposdates, 'id': [num_nodes+1]*len(colposdates), 'xz': [0]*len(colposdates), 'xy': [0]*len(colposdates)})
    for colpos_ts in colposdates:
        colpos_df = colpos_df.append(monitoring_vel.loc[monitoring_vel.ts == colpos_ts, ['ts', 'id', 'xz', 'xy']])
    colpos_df['x'] = colpos_df['id'].apply(lambda x: (num_nodes + 1 - x) * seg_len)
    colpos_df = colpos_df.sort('id', ascending = False)
    colpos_dfts = colpos_df.groupby('ts')
    colposdf = colpos_dfts.apply(col_pos, col_pos_end = window.end, col_pos_interval = config.io.col_pos_interval, col_pos_number = config.io.num_col_pos, num_nodes = num_nodes)


    # plot column position
    plot_column_positions(colposdf,colname,window.end, show_part_legend)

    lgd = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='medium')

    plt.savefig(output_path+config.io.outputfilepath+'realtime/'+colname+'ColPos_'+str(window.end.strftime('%Y-%m-%d_%H-%M'))+'.png',
                dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w', bbox_extra_artists=(lgd,), bbox_inches='tight')
    
    # plot displacement and velocity
    plot_disp_vel(monitoring_vel, colname, max_min_df, window, config, plotvel)
    plt.savefig(output_path+config.io.outputfilepath+'realtime/'+colname+'_DispVel_'+str(window.end.strftime('%Y-%m-%d_%H-%M'))+'.png',
                dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')

tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

for i in range(len(tableau20)):    
    r, green, b = tableau20[i]    
    tableau20[i] = (r / 255., green / 255., b / 255.)




##########################################################
###INPUTS
colname = 'pngta'
node = 8
axis = 'xz'
k = 3 #degree of spline
c = 1 #factor of error

#Step 1: Get dataframe for xz and xy using RealTimePlotter Code
col = q.GetSensorList(colname)

start = '2017-01-09 7:00:00'
end = '2017-01-10 09:00:00'


window, config = rtw.getwindow(pd.to_datetime(end))
config.io.to_smooth = 0
window.start = pd.to_datetime(start).to_datetime()


window.numpts = int(7)
window.offsetstart = window.start - timedelta(days=(config.io.num_roll_window_ops*window.numpts-1)/48.)

out_path = 'C:\Users\Win8\Documents\Dynaslope\\Data Analysis\\Filters\\Acceleration Velocity\\'
out_path = out_path + 'Underground\k {} Gaussian num_pts {}\\{}\\{}\\'.format(k,window.numpts,colname,str(node))
out_path1 = out_path + 'stats\\'
out_path2 = out_path + 'overall trend\\'
out_path3 = out_path + 'v vs a time evolution\\'

for paths in [out_path,out_path1,out_path2,out_path3]:
    if not os.path.exists(paths):
        os.makedirs(paths)


monitoring = g.genproc(col[0], window, config,'bottom')
data = monitoring.vel[monitoring.vel.id == node]
data = data[axis].reset_index().set_index(['ts'])
data['tvalue'] = data.index
data['delta'] = (data['tvalue']-data.index[0])
data['t'] = data['delta'].apply(lambda x: x  / np.timedelta64(1,'D'))
data['x'] = data[axis]

t = np.array(data['t'].values-data['t'][0])
x = np.array(data['x'].values)
timestamp = np.array(data['tvalue'].values)
time_start = data['tvalue'][0]

v = np.array([])
a = np.array([])

for i in np.arange(0,window.numpts-1):
    v = np.append(v,np.nan)
    a = np.append(a,np.nan)


for i in np.arange(window.numpts,len(t)+1):
#    if i != len(t)-20:
#        continue
    #data splicing    
    cur_t = t[i-window.numpts:i]
    cur_x = x[i-window.numpts:i]
    cur_timestamp = timestamp[i-window.numpts:i]
    
    #data spline
    try:
        #Take the gaussian average of data points and its variance
        _,var = moving_average(cur_x)
        sp = UnivariateSpline(cur_t,cur_x,w=c/np.sqrt(var))
        t_n = np.linspace(cur_t[0],cur_t[-1],1000)
        
        #spline results    
        x_n = sp(t_n)
        v_n = sp.derivative(n=1)(t_n)
        a_n = sp.derivative(n=2)(t_n)
        
        #compute for velocity (cm/day) vs. acceleration (cm/day^2) in log axes
        x_s = sp(cur_t)
        v_s = abs(sp.derivative(n=1)(cur_t) * 100)
        a_s = abs(sp.derivative(n=2)(cur_t) * 100)
    except:
        t_n = np.linspace(cur_t[0],cur_t[-1],1000)
        print "Interpolation Error {}".format(pd.to_datetime(str(cur_timestamp[-1])).strftime("%m/%d/%Y %H:%M"))
        x_n = np.ones(len(t_n))*np.nan        
        v_n = np.ones(len(t_n))*np.nan
        a_n = np.ones(len(t_n))*np.nan
        x_s = np.ones(len(cur_t))*np.nan
        v_s = np.ones(len(cur_t))*np.nan
        a_s = np.ones(len(cur_t))*np.nan
    
    
    v = np.append(v,v_s[-1])
    a = np.append(a,a_s[-1])    

data['v'] = v
data['a'] = a
v_min = min(v[~np.isnan(v)])
v_max = max(v[~np.isnan(v)])
v_theo = np.arange(v_min,v_max,0.0001)
a_theo, a_theo_up, a_theo_down = fukuzono_constants(v_min = v_min, v_max = v_max, numpts = len(v_theo))

for i in np.arange(window.numpts,len(t)+1):
    
    #Redundant Computation of Spline Results
    #Data Splicing
    cur_t = t[i-window.numpts:i]
    cur_x = x[i-window.numpts:i]
    cur_timestamp = timestamp[i-window.numpts:i]

    #Data Spline
    try:
        _,var = moving_average(cur_x)
        sp = UnivariateSpline(cur_t,cur_x,w=c/np.sqrt(var))
        t_n = np.linspace(cur_t[0],cur_t[-1],1000)
        
        #Spline Results
        x_n = sp(t_n)
        v_n = sp.derivative(n=1)(t_n)
        a_n = sp.derivative(n=2)(t_n)
        x_s = sp(cur_t)
        
    except:
        print "Interpolation Error {}".format(pd.to_datetime(str(cur_timestamp[-1])).strftime("%m/%d/%Y %H:%M"))
        x_n = np.ones(len(t_n))*np.nan
        v_n = np.ones(len(t_n))*np.nan
        a_n = np.ones(len(t_n))*np.nan
        x_s = np.ones(len(cur_t))*np.nan

    SS_res,r2,RMSE = goodness_of_fit(cur_t,cur_x,x_s)
    text = 'SSE = {} \nR-square = {} \nRMSE = {}'.format(round(SS_res,4),round(r2,4),round(RMSE,4))
    
    cur_data = data[:cur_timestamp[-1]].tail(window.numpts)
    cur_v,cur_a = cur_data.v.values,cur_data.a.values
    fig = plt.figure()
    fig.set_size_inches(15,8)
    ax1 = fig.add_subplot(121)
    ax1.get_xaxis().tick_bottom()    
    ax1.get_yaxis().tick_left()
    ax1.grid()
    ax1.fill_between(v_theo,a_theo_up,a_theo_down,facecolor = tableau20[1],alpha = 0.5)
    l1 = ax1.plot(v_theo,a_theo,c = tableau20[0],label = 'Fukuzono (1985)')
    ax1.plot(v_theo,a_theo_up,'--',c = tableau20[0])
    ax1.plot(v_theo,a_theo_down,'--', c = tableau20[0])
    ax1.plot(cur_v[-10:],cur_a[-10:],c = tableau20[10])
    ax1.plot(cur_v[-10:],cur_a[-10:],'o',c = tableau20[4],label = 'Data')
    
    ax1.set_xlabel('Velocity (cm/day)')
    ax1.set_ylabel('Acceleration (cm/day$^2$)')
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    
    ax1.legend(loc = 'upper left',fancybox = True, framealpha = 0.5)
    
    tsn3 = pd.to_datetime(str(cur_timestamp[-1])).strftime("%b %d, %Y %H:%M")
    
    tsn4 = pd.to_datetime(str(cur_timestamp[-1]))
    tsn4 = tsn4.strftime("%Y-%m-%d_%H-%M-%S")    
    
    fig.suptitle(colname.upper() + " Node " + str(node) + " Velocity and Acceleration" + " {}".format(tsn3))    
    
    ax2 = fig.add_subplot(222)
    ax2.grid()
    ax2.plot(cur_t,cur_x,'.',color = tableau20[0],label = 'Data')
    ax2.plot(t_n,x_n,color = tableau20[8],label = 'Interpolation')
    ax2.set_ylabel('Disp (meters)')
    props = dict(boxstyle = 'round',facecolor = 'white',alpha = 0.5)
    ax2.text(1-0.320, 0.664705882353-0.43,text,transform = ax2.transAxes,verticalalignment = 'top',horizontalalignment = 'left',bbox = props)        
    ax2.legend(loc = 'upper left',fancybox = True, framealpha = 0.5)
    
    #FIX THIS SHIT!!!!
    #Plot Velocity vs. Time    
    ax3 = fig.add_subplot(224,sharex = ax2)
    ax3.grid()
    ax3.plot(t_n,v_n,c = tableau20[4],label = 'Velocity')
    ax3.set_ylabel('Velocity (m/day)')
    ax3.set_xlabel('Time (days)')    
    ax3.legend(loc = 'upper left',fancybox = True, framealpha = 0.5)
    
    #Plot Acceleration vs. Time
    text = "v = {}\na= {}".format(round(v_n[-1],5),round(a_n[-1],5))
    ax4 = ax3.twinx()
    ax4.plot(t_n,a_n,c = tableau20[6],label = 'Acceleration')
    ax4.set_ylabel('Acceleration (m/day$^2$)')
    ax4.text(1-0.320, 0.664705882353-1.60,text,transform = ax2.transAxes,verticalalignment = 'top',horizontalalignment = 'left',bbox = props)
    ax4.legend(loc = 'upper right',fancybox = True, framealpha = 0.5)
    
    fig4_out_path = out_path3 + " {} ".format(tsn4) + colname + " " + str(node) + " velocity vs acceleration" +" {} {}".format(str(k),str(c).replace('.',''))
    
    plt.subplots_adjust(left = 0.09, right = 0.87, wspace = 0.20)
    plt.savefig(fig4_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches = 'tight')
    plt.close()    
    
fig = plt.figure()
fig.set_size_inches(10,8)
ax = fig.add_subplot(111)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.get_xaxis().tick_bottom()    
ax.get_yaxis().tick_left() 
ax.grid()
ax.fill_between(v_theo,a_theo_up,a_theo_down,facecolor = tableau20[1],alpha = 0.5)
ax.plot(v_theo,a_theo,c=tableau20[0],label = 'Fukuzono (1985)')
ax.plot(v_theo,a_theo_up,'--',c=tableau20[0])
ax.plot(v_theo,a_theo_down,'--',c=tableau20[0])
ax.plot(v,a,'o',c=tableau20[4],label = 'Data Points')
ax.set_xlabel('Velocity (cm/day)',fontsize = 15)
ax.set_ylabel('Acceleration (cm/day$^2$)',fontsize = 15)
ax.set_xscale('log')
ax.set_yscale('log')
ax.legend(loc = 'upper left',fancybox = True)
ax.set_title(colname.upper() + " Crack " + str(node) + " Velocity vs. Acceleration",fontsize = 18)

fig3_out_path = out_path + " " + colname + " " + str(node) + " velocity vs acceleration" +" {} {}".format(str(k),str(c).replace('.',''))
plt.savefig(fig3_out_path,facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches='tight')

