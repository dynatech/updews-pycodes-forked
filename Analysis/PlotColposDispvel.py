##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ion()

import pandas as pd
import numpy as np
from datetime import datetime
import cfgfileio as cfg

def compute_col_pos(df):
    
    cumsum_df = df.loc[df.ts == df.ts.values[0]][['xz','xy']].cumsum()
    df['cs_xz'] = cumsum_df.xz.values
    df['cs_xy'] = cumsum_df.xy.values

    return np.round(df, 4)
                
def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_color_cycle([cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in range(NUM_COLORS)])
    return ax
    
def subplot_colpos(dfts, ax_xz, ax_xy):
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
    curax.plot(curcolpos_xy,curcolpos_x,'.-', label=str(pd.to_datetime(dfts.ts.values[0])))
    curax.set_xlabel('xy')
    return
    
    
def plot_column_positions(df,colname,end):
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
        
#        dfts = df.groupby('ts')
#        dfts.apply(subplot_colpos, ax_xz=ax_xz, ax_xy=ax_xy)
        
        for i in set(df.ts):
            subplot_colpos(df.loc[df.ts == i], ax_xz=ax_xz, ax_xy=ax_xy)

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
    
        fig.tight_layout()
        fig.subplots_adjust(top=0.9)        
        fig.suptitle(colname+" as of "+str(end),fontsize='medium')
        
        plt.legend(fontsize='x-small')        
    
    except:        
        print colname, "ERROR in plotting column position"
    return ax_xz,ax_xy
    
def vel_classify(vel):
    velplot=pd.DataFrame(index=vel.index)
    for n in vel.columns:
#        velplot[n]=np.ones(len(vel))*(len(vel.columns)-n)
        velplot[n]=n
    try:        
        L2mask=(vel.abs()>T_velL2)&(vel.abs()<=T_velL3)
        L3mask=(vel.abs()>T_velL3) 
        
        L2=velplot[L2mask]
        L3=velplot[L3mask]
                
        return velplot,L2,L3
    except:
        print "ERROR computing velocity classification ###################################"
        return 
    
def plot_disp_vel(colname, xz,xy,xz_vel,xy_vel,
                  xz_mx=0,xz_mn=0,xy_mx=0,xy_mn=0, 
                  disp_offset='max',disp_zero=True):
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


    #setting up zeroing and offseting parameters
    
    if disp_offset=='max':
        xzd_plotoffset=(xz.max()-xz.min()).max()
        disp_zero=True
    elif disp_offset=='mean':
        xzd_plotoffset=(xz.max()-xz.min()).mean()
        disp_zero=True
    elif disp_offset=='min':
        xzd_plotoffset=(xz.max()-xz.min()).min()
        disp_zero=True
    else:
        xzd_plotoffset=0
        
   

    #creating noise envelope
#    check if xz_mx, xz_mn, xy_mx, xy_mn are all arrays
    try:
        len(xz_mx)
        len(xz_mn)
        len(xy_mx)
        len(xy_mn)
    except:
        xz_mx=np.ones(len(xz.columns)+1)*np.nan
        xz_mn=np.ones(len(xz.columns)+1)*np.nan
        xy_mx=np.ones(len(xz.columns)+1)*np.nan
        xy_mn=np.ones(len(xz.columns)+1)*np.nan
        
    try:
        print np.where(xz_mx==xz_mn).all()
        xz_mx=np.ones(len(xz.columns)+1)*np.nan
        xz_mn=np.ones(len(xz.columns)+1)*np.nan
        print xz_mx
        
    except:
        if disp_zero:
            xz_first_row=xz.loc[(xz.index==xz.index[0])].values.squeeze()
            xz_mx0=np.subtract(xz_mx[:-1],xz_first_row)
            xz_mn0=np.subtract(xz_mn[:-1],xz_first_row)
        else:
            xz_mx0=xz_mx[:-1]
            xz_mn0=xz_mn[:-1]

    try:
        print np.where(xy_mx==xy_mn).all()
        xy_mx=np.ones(len(xz.columns)+1)*np.nan
        xy_mn=np.ones(len(xz.columns)+1)*np.nan
        print xy_mx
        
    except:
        if disp_zero:
            xy_first_row=xy.loc[(xy.index==xy.index[0])].values.squeeze()
            xy_mx0=np.subtract(xy_mx[:-1],xy_first_row)
            xy_mn0=np.subtract(xy_mn[:-1],xy_first_row)
        else:
            xy_mx0=xy_mx[:-1]
            xy_mn0=xy_mn[:-1]
    
    xz_u=pd.DataFrame(index=xz.index)
    xz_l=pd.DataFrame(index=xz.index)
    xy_u=pd.DataFrame(index=xy.index)
    xy_l=pd.DataFrame(index=xy.index)

    for h in xz.columns:
        if xz_mx0[h-1]==xz_mn0[h-1]:
            xz_u[h]=np.nan
            xz_l[h]=np.nan
        else:
            xz_u[h]=xz_mx0[h-1]
            xz_l[h]=xz_mn0[h-1]
        if xy_mx0[h-1]==xy_mn0[h-1]:
            xy_u[h]=np.nan
            xy_l[h]=np.nan
        else:
            xy_u[h]=xy_mx0[h-1]
            xy_l[h]=xy_mn0[h-1]
        
#        xz_u[h]=xz_mx0[h-1]
#        xz_l[h]=xz_mn0[h-1]
#        xy_u[h]=xy_mx0[h-1]
#        xy_l[h]=xy_mn0[h-1]
    
    xz_u=df_add_offset_col(xz_u,xzd_plotoffset)
    xz_l=df_add_offset_col(xz_l,xzd_plotoffset)
    xy_u=df_add_offset_col(xy_u,xzd_plotoffset)
    xy_l=df_add_offset_col(xy_l,xzd_plotoffset)
    
    #zeroing and offseting xz,xy
    if disp_zero:
        xz=df_add_offset_col(df_zero_initial_row(xz),xzd_plotoffset)
        xy=df_add_offset_col(df_zero_initial_row(xy),xzd_plotoffset)
    else:
        xz=df_add_offset_col(xz,xzd_plotoffset)
        xy=df_add_offset_col(xy,xzd_plotoffset)
    
    try:
        fig=plt.figure()
       
        
        #creating subplots        
        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
        ax_xzv=fig.add_subplot(143)
        ax_xzv.invert_yaxis()
        ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)
       
        #assigning non-repeating colors to subplots axis
        ax_xzd=nonrepeat_colors(ax_xzd,len(xz.columns))
        ax_xyd=nonrepeat_colors(ax_xyd,len(xz.columns))
        ax_xzv=nonrepeat_colors(ax_xzv,len(xz.columns))
        ax_xyv=nonrepeat_colors(ax_xyv,len(xz.columns))
    
        
        #plotting displacement for xz
        curax=ax_xzd
        plt.sca(curax)
        xz.plot(ax=curax,legend=False)
        xz_u.plot(ax=curax,ls=':',legend=False)
        xz_l.plot(ax=curax,ls=':',legend=False)
        curax.set_title('3-day displacement\n XZ axis',fontsize='small')
        curax.set_ylabel('displacement scale, m', fontsize='small')
        y = xz.iloc[0].values
        x = xz.index[0]
        z = xz.columns
        for i,j in zip(y,z):
           curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')
        
        #plotting displacement for xy
        curax=ax_xyd
        plt.sca(curax)
        xy.plot(ax=curax,legend=False)
        xy_u.plot(ax=curax,ls=':',legend=False)
        xy_l.plot(ax=curax,ls=':',legend=False)
        curax.set_title('3-day displacement\n XY axis',fontsize='small')
        y = xy.iloc[0].values
        x = xy.index[0]
        z = xy.columns
        for i,j in zip(y,z):
           curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')
        
        #plotting velocity for xz
        curax=ax_xzv
        plt.sca(curax)
        try:        
            velplot,L2,L3=vel_classify(xz_vel)            
            velplot.plot(ax=curax,marker='.',legend=False)
            L2.plot(ax=curax,marker='^',ms=8,mfc='y',lw=0,legend=False)
            L3.plot(ax=curax,marker='^',ms=10,mfc='r',lw=0,legend=False)
            
            y = velplot.iloc[0].values
            x = velplot.index[0]
            z = velplot.columns
            for i,j in zip(y,z):
                curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')            
            curax.set_ylabel('node ID', fontsize='small')
            curax.set_title('3-hr velocity alerts\n XZ axis',fontsize='small')
        except: 
            print "ERROR plotting xz velocity class"    
            
        
        #plotting velocity for xz        
        curax=ax_xyv
        plt.sca(curax)
        try:        
            velplot,L2,L3=vel_classify(xy_vel)            
            velplot.plot(ax=curax,marker='.',legend=False)
            L2.plot(ax=curax,marker='^',ms=8,mfc='y',lw=0,legend=False)
            L3.plot(ax=curax,marker='^',ms=10,mfc='r',lw=0,legend=False)
            
            y = velplot.iloc[0].values
            x = velplot.index[0]
            z = velplot.columns
            for i,j in zip(y,z):
                curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')            
            curax.set_title('3-hr velocity alerts\n XY axis',fontsize='small')            
        except:
            print "ERROR plotting xz velocity class"
            
            
        # rotating xlabel
        
        for tick in ax_xzd.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
            
        for tick in ax_xyd.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xzv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xyv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
#    
        for tick in ax_xzd.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
            
        for tick in ax_xyd.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xzv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xyv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
            
        fig.tight_layout()
        
        fig.subplots_adjust(top=0.85)        
        fig.suptitle(colname+" as of "+str(velplot.index[-1]),fontsize='medium')
        line=mpl.lines.Line2D((0.5,0.5),(0.1,0.8))
        fig.lines=line,
        
    except:      
        print colname, "ERROR in plotting displacements and velocities"
    return

def main(monitoring, window, config):

    colname = monitoring.colprops.name
    num_nodes=monitoring.colprops.nos
    seg_len=monitoring.colprops.seglen
    monitoring_vel = monitoring.vel.reset_index()
    print monitoring_vel
    
    colposdates = pd.date_range(end=window.end, freq=cfg.config().io.col_pos_interval,periods=cfg.config().io.num_col_pos, name='ts',closed=None)
    print colposdates
    colpos_df = pd.DataFrame({'ts': colposdates, 'id': [num_nodes+1]*len(colposdates), 'xz': [0]*len(colposdates), 'xy': [0]*len(colposdates)})
    for colpos_ts in colposdates:
        colpos_df = colpos_df.append(monitoring_vel.loc[monitoring_vel.ts == colpos_ts, ['ts', 'id', 'xz', 'xy']])
    print colpos_df
    colpos_df['x'] = colpos_df['id'].apply(lambda x: (num_nodes + 1 - x) * seg_len)
    colpos_df = colpos_df.sort('id', ascending = False)
    colpos_dfts = colpos_df.groupby('ts')
    colposdf = colpos_dfts.apply(compute_col_pos)
    
    
    #11. Plotting column positions
    ax=plot_column_positions(colposdf,colname, window.end)
    
#    #plotting error band for column positions        
#    try:
#        xl=cs_x.mean().values               
#        ax[0].fill_betweenx(xl[::-1], xz_mxc, xz_mnc, where=xz_mxc >= xz_mnc, facecolor='0.7',linewidth=0)
#        ax[1].fill_betweenx(xl[::-1], xy_mxc, xy_mnc, where=xy_mxc >= xy_mnc, facecolor='0.7',linewidth=0)
#    except KeyboardInterrupt:
#        print 'no column position error band'
    plt.savefig(colname+' colpos '+str(window.end.strftime('%Y-%m-%d %H-%M')),
                dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
    
#    
#    #12. Plotting displacement and velocity
#    plot_disp_vel(colname, xz,xy, vel_xz, vel_xy,xz_mx,xz_mn,xy_mx,xy_mn)
#    plt.savefig(RTfilepath+colname+' disp_vel '+str(end.strftime('%Y-%m-%d %H-%M')),
#                dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')


