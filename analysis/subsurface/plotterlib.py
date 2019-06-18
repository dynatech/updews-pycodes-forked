##### IMPORTANT matplotlib declarations must always be FIRST to make sure that
##### matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ion()

from datetime import timedelta
import numpy as np
import os
import pandas as pd
from scipy.stats import spearmanr
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb


mpl.rcParams['xtick.labelsize'] = 'xx-small'
mpl.rcParams['ytick.labelsize'] = 'small'
mpl.rcParams['axes.labelsize'] = 'medium'
mpl.rcParams['figure.titlesize'] = 'x-large'
mpl.rcParams['legend.fontsize'] = 'small'


def col_pos(colpos_dfts):  
    colpos_dfts = colpos_dfts.drop_duplicates()
    cumsum_df = colpos_dfts[['xz','xy']].cumsum()
    colpos_dfts.loc[:, 'cs_xz'] = cumsum_df['xz'].values
    colpos_dfts.loc[:, 'cs_xy'] = cumsum_df['xy'].values
    return np.round(colpos_dfts, 4)

def compute_depth(colpos_dfts):
    colpos_dfts = colpos_dfts.drop_duplicates()
    cumsum_df = colpos_dfts[['x']].cumsum()
    cumsum_df.loc[:, 'x'] = cumsum_df['x'] - min(cumsum_df.x)
    colpos_dfts.loc[:, 'x'] = cumsum_df['x'].values
    return np.round(colpos_dfts, 4)

def adjust_depth(colpos_dfts, max_depth):
    depth = max_depth - max(colpos_dfts['x'].values)
    colpos_dfts.loc[:, 'x'] = colpos_dfts['x'] + depth
    return colpos_dfts

def compute_colpos(window, sc, tilt, tsm_props):
    colposdate = pd.date_range(end=window.end,
                                freq=sc['subsurface']['col_pos_interval'],
                                periods=sc['subsurface']['num_col_pos'],
                                name='ts', closed=None)
    colpos_df = tilt[tilt.ts.isin(colposdate)].copy()
    colpos_df.loc[:, 'x'] = np.sqrt(tsm_props.seglen**2 - np.power(colpos_df['xz'], 2)
                                - np.power(colpos_df['xy'], 2))
    colpos_df.loc[:, 'x'] = colpos_df['x'].fillna(tsm_props.seglen)
    
    if sc['subsurface']['column_fix'] == 'top':
        colpos_df0 = pd.DataFrame({'ts': colposdate, 'node_id':
                                [0]*len(colposdate), 'xz': [0]*len(colposdate),
                                'xy': [0]*len(colposdate), 'x':
                                [0]*len(colposdate)})
    elif sc['subsurface']['column_fix'] == 'bottom':
        colpos_df0 = pd.DataFrame({'ts': colposdate, 'node_id':
                                [tsm_props.nos+1]*len(colposdate), 'xz':
                                [0]*len(colposdate), 'xy': [0]*len(colposdate),
                                'x': [tsm_props.seglen]*len(colposdate)})
    
    colpos_df = colpos_df.append(colpos_df0, ignore_index = True, sort=False)
    
    if sc['subsurface']['column_fix'] == 'top':
        colpos_df = colpos_df.sort_values('node_id', ascending = True)
    elif sc['subsurface']['column_fix'] == 'bottom':
        colpos_df = colpos_df.sort_values('node_id', ascending = False)
    
    colpos_dfts = colpos_df.groupby('ts', as_index=False)
    colposdf = colpos_dfts.apply(col_pos).reset_index(drop=True)
    
    colposdf = colposdf.sort_values('node_id', ascending = True)

    colpos_dfts = colposdf.groupby('ts', as_index=False)
    colposdf = colpos_dfts.apply(compute_depth).reset_index(drop=True)
    
    if sc['subsurface']['column_fix'] == 'bottom':
        max_depth = max(colposdf['x'].values)
        colposdfts = colposdf.groupby('ts')
        colposdf = colposdfts.apply(adjust_depth, max_depth=max_depth)
    
    colposdf.loc[:, 'x'] = colposdf['x'].apply(lambda x: -x)

    return colposdf

def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_prop_cycle(color=[cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in
                                range(NUM_COLORS+1)[::-1]])
    return ax
    
    
def subplot_colpos(dfts, ax_xz, ax_xy, show_part_legend, sc, colposTS):
    i = colposTS.loc[colposTS.ts == dfts['ts'].values[0]]['index'].values[0]
    
    #current column position x
    curcolpos_x = dfts['x'].values

    #current column position xz
    curax = ax_xz
    curcolpos_xz = dfts['cs_xz'].apply(lambda x: x*1000).values
    curax.plot(curcolpos_xz,curcolpos_x,'.-')
    curax.set_xlabel('horizontal displacement, \n downslope(mm)')
    curax.set_ylabel('depth, m')
    
    #current column position xy
    curax=ax_xy
    curcolpos_xy = dfts['cs_xy'].apply(lambda x: x*1000).values
    if show_part_legend == False:
        curax.plot(curcolpos_xy, curcolpos_x, '.-',
                   label=str(pd.to_datetime(dfts['ts'].values[0])))
    else:
        if i % show_part_legend == 0 or i == sc['subsurface']['num_col_pos'] - 1:
            curax.plot(curcolpos_xy, curcolpos_x, '.-',
                       label=str(pd.to_datetime(dfts['ts'].values[0])))
        else:
            curax.plot(curcolpos_xy,curcolpos_x,'.-')
    curax.set_xlabel('horizontal displacement, \n across slope(mm)')
    
def plot_column_positions(df, tsm_props, window, sc, show_part_legend,
                          max_min_cml=''):
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
    
        ax_xz=nonrepeat_colors(ax_xz,len(set(df['ts'].values)),color='plasma')
        ax_xy=nonrepeat_colors(ax_xy,len(set(df['ts'].values)),color='plasma')
    
        colposTS = pd.DataFrame({'ts': sorted(set(df.ts)), 'index':
            range(len(set(df.ts)))})
        
        dfts = df.groupby('ts')
        dfts.apply(subplot_colpos, ax_xz=ax_xz, ax_xy=ax_xy,
                   show_part_legend=show_part_legend, sc=sc, colposTS=colposTS)
    
#        try:
#            max_min_cml = max_min_cml.apply(lambda x: x*1000)
#            xl = df.loc[(df.ts == window.end)&(df.node_id <= tsm_props.nos)&(df.node_id >= 1)]['x'].values[::-1]
#            ax_xz.fill_betweenx(xl, max_min_cml['xz_maxlist'].values, max_min_cml['xz_minlist'].values, where=max_min_cml['xz_maxlist'].values >= max_min_cml['xz_minlist'].values, facecolor='0.7',linewidth=0)
#            ax_xy.fill_betweenx(xl, max_min_cml['xy_maxlist'].values, max_min_cml['xy_minlist'].values, where=max_min_cml['xy_maxlist'].values >= max_min_cml['xy_minlist'].values, facecolor='0.7',linewidth=0)
#        except:
#            qdb.print_out('error in plotting noise env')
    
        for tick in list(ax_xz.xaxis.get_minor_ticks()) \
                + list(ax_xy.xaxis.get_minor_ticks()) \
                + list(ax_xz.xaxis.get_major_ticks()) \
                + list(ax_xy.xaxis.get_major_ticks()):          
            tick.label.set_rotation('vertical')

        plt.subplots_adjust(top=0.92, bottom=0.15, left=0.10, right=0.73)        
        plt.suptitle(tsm_props.tsm_name)
        ax_xz.grid(True)
        ax_xy.grid(True)

    except:        
        qdb.print_out(tsm_props.tsm_name + " ERROR in plotting column position")

    return ax_xz,ax_xy

def vel_plot(df, velplot, num_nodes):
    velplot.loc[:, df['node_id'].values[0]] = num_nodes - df['node_id'].values[0] + 1
    return velplot

def vel_classify(df, sc, num_nodes, linearvel=True):
    if linearvel:
        vel=pd.DataFrame(index=sorted(set(df.ts)))
        nodal_df = df.groupby('node_id')
        velplot = nodal_df.apply(vel_plot, velplot=vel, num_nodes=num_nodes)
        velplot = velplot.reset_index()
        velplot = velplot.loc[velplot.node_id == len(set(df.node_id))]
        velplot = velplot[['level_1'] + list(range(1, len(set(df.node_id))+1))]
        velplot = velplot.rename(columns = {'level_1': 'ts'}).set_index('ts')
    else:
        velplot = ''
    df = df.set_index(['ts', 'node_id'])
    vel2 = float(sc['subsurface']['vel2'])
    vel3 = float(sc['subsurface']['vel3'])
    L2mask = (df.abs() > vel2) & (df.abs() <= vel3)
    L3mask = (df.abs() > vel3)
    L2mask = L2mask.reset_index().replace(False, np.nan)
    L3mask = L3mask.reset_index().replace(False, np.nan)
    L2mask = L2mask.dropna()[['ts', 'node_id']]
    L3mask = L3mask.dropna()[['ts', 'node_id']]
    return velplot, L2mask, L3mask
    
def noise_env_df(df, tsdf):
    df.loc[:, 'ts'] = tsdf
    return df

def plotoffset(df, disp_offset = 'mean'):
    #setting up zeroing and offseting parameters
    nodal_df = df.groupby('node_id')

    if disp_offset == 'max':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).max()
    elif disp_offset == 'mean':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).mean()
    elif disp_offset == 'min':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).min()
    else:
        xzd_plotoffset = 0
    
    return xzd_plotoffset

def cum_surf(df, xzd_plotoffset, num_nodes):
    # defining cumulative (surface) displacement
    dfts = df.groupby('ts')
    cs_df = dfts[['xz', 'xy']].sum()    
    cs_df = cs_df - cs_df.values[0] + xzd_plotoffset * num_nodes
    cs_df = cs_df.sort_index()
    
    return cs_df

def noise_env(df, max_min_df, window, num_nodes, xzd_plotoffset):
    max_min_df = max_min_df.sort_index()
    
    #creating noise envelope
    first_row = df.loc[df.ts == window.start].sort_values('node_id')
    first_row = first_row.set_index('node_id')[['xz', 'xy']]
    
    for axis in ['xy', 'xz']:
        max_min_df.loc[:, axis+'_maxlist'] = (max_min_df[axis+'_maxlist'] - first_row[axis]) + ((num_nodes - max_min_df.index) * xzd_plotoffset)
        max_min_df.loc[:, axis+'_minlist'] = (max_min_df[axis+'_minlist'] - first_row[axis]) + ((num_nodes - max_min_df.index) * xzd_plotoffset)
        
    max_min_df = max_min_df.reset_index()
    noise_df = max_min_df.append(max_min_df, ignore_index=True, sort=False)
    tsdf = [window.start]*len(max_min_df)+[window.end]*len(max_min_df)
    noise_df.loc[:, 'ts'] = tsdf
    noise_df = noise_df.set_index('ts')

    return noise_df

def disp0off(df, window, sc, xzd_plotoffset, num_nodes):
    if sc['subsurface']['column_fix'] == 'top':
        df.loc[:, 'xz'] = df['xz'].apply(lambda x: -x)
        df.loc[:, 'xy'] = df['xy'].apply(lambda x: -x)
    nodal_df = df.groupby('node_id')
    df0 = nodal_df.apply(df_zero_initial_row, window = window)
    nodal_df0 = df0.groupby('node_id', as_index=False)
    df0off = nodal_df0.apply(df_add_offset_col, offset = xzd_plotoffset,
                             num_nodes = num_nodes)

    # conpensates double offset of node 1 due to df.apply
    a = df0off.loc[df0off.node_id == 1].copy()
    a.loc[:, ['xz', 'xy']] = a[['xz', 'xy']] - (num_nodes - 1) * xzd_plotoffset
    df0off = df0off.loc[df0off.node_id != 1]
    df0off = df0off.append(a, sort=False)
    df0off = df0off.sort_values('ts')

    return df0off

def check_increasing(df, inc_df):
    sum_index = inc_df.loc[inc_df.node_id == df['node_id'].values[0]].index[0]
    sp, pval = spearmanr(range(len(df)), df['xz'].values)
    if sp > 0.5:
        inc_xz = int(10 * (round(abs(sp), 1) - 0.5))
    else:
        inc_xz = 0
    sp, pval = spearmanr(range(len(df)), df['xy'].values)
    if sp > 0.5:
        inc_xy = int(10 * (round(abs(sp), 1) - 0.5))
    else:
        inc_xy = 0
    diff_xz = max(df['xz'].values) - min(df['xz'].values)
    diff_xy = max(df['xy'].values) - min(df['xy'].values)
    inc_df.loc[sum_index] = [df['node_id'].values[0], inc_xz, inc_xy, diff_xz, diff_xy]

def metadata(inc_df):
    node_id = str(int(inc_df['node_id'].values[0]))

    if inc_df['diff_xz'].values[0]>0.01:
        if inc_df['inc_xz'].values[0]>3:
            text_xz = node_id + '++++'
            xz_text_size = 'large'
        elif inc_df['inc_xz'].values[0]>2:
            text_xz = node_id + '+++'
            xz_text_size = 'large'
        elif inc_df['inc_xz'].values[0]>1:
            text_xz = node_id + '++'
            xz_text_size = 'medium'
        elif inc_df['inc_xz'].values[0]>0:
            text_xz = node_id + '+'
            xz_text_size = 'medium'
        else:
            text_xz = node_id
            xz_text_size = 'x-small'
    else:
        text_xz = node_id
        xz_text_size = 'x-small'
    
    if inc_df['diff_xy'].values[0]>0.01:
        if inc_df['inc_xy'].values[0]>3:
            text_xy = node_id + '++++'
            xy_text_size = 'large'
        elif inc_df['inc_xy'].values[0]>2:
            text_xy = node_id + '+++'
            xy_text_size = 'large'
        elif inc_df['inc_xy'].values[0]>1:
            text_xy = node_id + '++'
            xy_text_size = 'medium'
        elif inc_df['inc_xy'].values[0]>0:
            text_xy = node_id + '+'
            xy_text_size = 'medium'
        else:
            text_xy = node_id
            xy_text_size = 'x-small'
    else:
        text_xy = node_id
        xy_text_size = 'x-small'
    
    inc_df.loc[:, 'text_xz'] = text_xz
    inc_df.loc[:, 'xz_text_size'] = xz_text_size
    inc_df.loc[:, 'text_xy'] = text_xy
    inc_df.loc[:, 'xy_text_size'] = xy_text_size

    return inc_df

def node_annotation(df, num_nodes):
    check_inc_df = df.sort_values('ts')
    
    inc_df = pd.DataFrame({'node_id': range(1, num_nodes+1), 'inc_xz': [np.nan]*num_nodes, 'inc_xy': [np.nan]*num_nodes, 'diff_xz': [np.nan]*num_nodes, 'diff_xy': [np.nan]*num_nodes})
    inc_df = inc_df[['node_id', 'inc_xz', 'inc_xy', 'diff_xz', 'diff_xy']]
    nodal_monitoring_vel = check_inc_df.groupby('node_id')
    nodal_monitoring_vel.apply(check_increasing, inc_df=inc_df)
    
    nodal_inc_df = inc_df.groupby('node_id', as_index=False)
    inc_df = nodal_inc_df.apply(metadata)
    
    return inc_df

def plot_annotation(curax, axis, df0off, inc_df, plot_inc):
    y = df0off.loc[df0off.index == min(df0off.index)].sort_values('node_id')[axis].values
    x = min(df0off.index)
    z = range(1, len(y)+1)
    if not plot_inc:
        for i,j in zip(y,z):
            curax.annotate(str(int(j)), xy=(x,i), xytext = (5,-2.5),
                          textcoords='offset points',size = 'x-small')
    else:
        for i,j in zip(y,z):
            text = inc_df.loc[inc_df.node_id == j]['text_'+axis].values[0]
            text_size = inc_df.loc[inc_df.node_id == j][axis+'_text_size'].values[0]
            curax.annotate(text,xy=(x,i),xytext = (5,-2.5), textcoords='offset points', size = text_size )

def plot_noise_env(curax, axis, noise_df):
    nodal_noise_df = noise_df.groupby('node_id')
    nodal_noise_df[axis+'_maxlist'].apply(curax.plot, ls=':')
    nodal_noise_df[axis+'_minlist'].apply(curax.plot, ls=':')

def plot_disp(curax, axis, df0off):
    plt.sca(curax)
    nodal_df0off = df0off.groupby('node_id')
    nodal_df0off[axis].apply(plt.plot)

def plot_disp_vel(noise_df, df0off, cs_df, colname, window, sc, plotvel,
                  xzd_plotoffset, num_nodes, velplot, plot_inc, inc_df=''):
#==============================================================================
# 
#     DESCRIPTION:
#     returns plot of xz & xy displacements per node, xz & xy velocities per node
# 
#     INPUT:
#     xz; array of floats; linear displacements along the planes defined by xa-za
#     xy; array of floats; linear displacements along the planes defined by xa-ya
#     xz_vel; array of floats; velocity along the planes defined by xa-za
#     xy_vel; array of floats; velocity along the planes defined by xa-ya
#==============================================================================

    if plotvel:
        vel_xz, vel_xy, L2_xz, L2_xy, L3_xz, L3_xy = velplot

    df0off = df0off.set_index('ts')
    
    fig=plt.figure()

    try:
        if plotvel:
            #creating subplots        
            ax_xzd=fig.add_subplot(141)
            ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
            ax_xzd.grid(True)
            ax_xyd.grid(True)
            
            ax_xzv=fig.add_subplot(143)
            ax_xzv.invert_yaxis()
            ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)
        else:
            #creating subplots        
            ax_xzd=fig.add_subplot(121)
            ax_xyd=fig.add_subplot(122,sharex=ax_xzd,sharey=ax_xzd)
            ax_xzd.grid(True)
            ax_xyd.grid(True)
    except:
        if plotvel:
            #creating subplots                      
            ax_xzv=fig.add_subplot(121)
            ax_xzv.invert_yaxis()
            ax_xyv=fig.add_subplot(122,sharex=ax_xzv,sharey=ax_xzv)
    
    try:
        #plotting cumulative (surface) displacments
        ax_xzd.plot(cs_df.index, cs_df['xz'].values, color='0.4', linewidth=0.5)
        ax_xyd.plot(cs_df.index, cs_df['xy'].values, color='0.4', linewidth=0.5)
        ax_xzd.fill_between(cs_df.index, cs_df.xz, xzd_plotoffset*(num_nodes),
                            color='0.8')
        ax_xyd.fill_between(cs_df.index, cs_df.xy, xzd_plotoffset*(num_nodes),
                            color='0.8')
    except:
        qdb.print_out('Error in plotting cumulative surface displacement')
        
    try:
        #assigning non-repeating colors to subplots axis
        ax_xzd=nonrepeat_colors(ax_xzd,num_nodes)
        ax_xyd=nonrepeat_colors(ax_xyd,num_nodes)
    except:
        qdb.print_out('Error in assigning non-repeating colors in displacement')
    
    if plotvel:
        ax_xzv=nonrepeat_colors(ax_xzv,num_nodes)
        ax_xyv=nonrepeat_colors(ax_xyv,num_nodes)

    #plotting displacement for xz
    curax=ax_xzd
    plot_disp(curax, 'xz', df0off)
    plot_noise_env(curax, 'xz', noise_df)
    plot_annotation(curax, 'xz', df0off, inc_df, plot_inc)
    curax.set_title('displacement\n downslope', fontsize='medium')
    curax.set_ylabel('displacement scale, m')

    #plotting displacement for xy
    curax=ax_xyd
    plot_disp(curax, 'xy', df0off)
    plot_noise_env(curax, 'xy', noise_df)
    plot_annotation(curax, 'xy', df0off, inc_df, plot_inc)
    curax.set_title('displacement\n across slope', fontsize='medium')
           
    if plotvel:
        #plotting velocity for xz
        curax=ax_xzv

        vel_xz.plot(ax=curax, marker='.', legend=False)

        L2_xz = L2_xz.sort_values('ts', ascending = True).set_index('ts')
        nodal_L2_xz = L2_xz.groupby('node_id')
        nodal_L2_xz.apply(lambda x: x['node_id'].plot(marker='^', ms=8, mfc='y',
                          lw=0,ax = curax))

        L3_xz = L3_xz.sort_values('ts', ascending = True).set_index('ts')
        nodal_L3_xz = L3_xz.groupby('node_id')
        nodal_L3_xz.apply(lambda x: x['node_id'].plot(marker='^', ms=10, mfc='r',
                          lw=0,ax = curax))
        
        y = sorted(range(1, num_nodes+1), reverse = True)
        x = (vel_xz.index)[1]
        z = sorted(range(1, num_nodes+1), reverse = True)
        for i,j in zip(y,z):
            curax.annotate(str(int(j)), xy=(x,i), xytext = (5,-2.5),
                           textcoords='offset points',size = 'x-small')            
        curax.set_ylabel('node ID')
        curax.set_title('velocity alerts\n downslope', fontsize='medium')  
    
        #plotting velocity for xy        
        curax=ax_xyv

        vel_xy.plot(ax=curax, marker='.', legend=False)
        
        L2_xy = L2_xy.sort_values('ts', ascending = True).set_index('ts')
        nodal_L2_xy = L2_xy.groupby('node_id')
        nodal_L2_xy.apply(lambda x: x['node_id'].plot(marker='^', ms=8, mfc='y',
                          lw=0,ax = curax))

        L3_xy = L3_xy.sort_values('ts', ascending = True).set_index('ts')
        nodal_L3_xy = L3_xy.groupby('node_id')
        nodal_L3_xy.apply(lambda x: x['node_id'].plot(marker='^', ms=10, mfc='r',
                          lw=0,ax = curax))
               
        y = range(1, num_nodes+1)
        x = (vel_xy.index)[1]
        z = range(1, num_nodes+1)
        for i,j in zip(y,z):
            curax.annotate(str(int(j)), xy=(x,i), xytext = (5,-2.5),
                           textcoords='offset points',size = 'x-small')            
        curax.set_title('velocity alerts\n across slope', fontsize='medium')                        
        
    # rotating xlabel
    for tick in list(ax_xzd.xaxis.get_minor_ticks()) \
            + list(ax_xyd.xaxis.get_minor_ticks()) \
            + list(ax_xzd.xaxis.get_major_ticks()) \
            + list(ax_xyd.xaxis.get_major_ticks()):
        tick.label.set_rotation('vertical')

    if plotvel:
        for tick in list(ax_xzv.xaxis.get_minor_ticks()) \
                + list(ax_xyv.xaxis.get_minor_ticks()) \
                + list(ax_xzv.xaxis.get_major_ticks()) \
                + list(ax_xyv.xaxis.get_major_ticks()):
            tick.label.set_rotation('vertical')

    try:
        dfmt = md.DateFormatter('%Y-%m-%d\n%H:%M')
        ax_xzd.xaxis.set_major_formatter(dfmt)
        ax_xyd.xaxis.set_major_formatter(dfmt)
    except:
        qdb.print_out('Error in setting date format of x-label in disp subplots')

    fig.set_tight_layout(True)
    
    fig.subplots_adjust(top=0.85)
    fig.suptitle(colname)

def df_zero_initial_row(df, window):
    #zeroing time series to initial value;
    #essentially, this subtracts the value of the first row
    #from all the rows of the dataframe
    for m in df.columns:
        if m not in ['ts', 'node_id']:
            df.loc[:, m] = df[m] - df.loc[df.ts == window.start][m].values[0]
    return np.round(df,4)

def df_add_offset_col(df, offset, num_nodes):
    #adding offset value based on column value (node ID);
    #topmost node (node 1) has largest offset
    for m in df.columns:
        if m not in ['ts', 'node_id']:
            df.loc[:, m] = df[m] + (num_nodes - df['node_id'].values[0]) * offset
    return np.round(df,4)
    
    
def main(data, tsm_props, window, sc, plotvel=True, show_part_legend = False,
         realtime=True, plot_inc=True, three_day_window=True,
         mirror_xz=False, mirror_xy=False):

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

    if realtime:
        plot_path = sc['fileio']['realtime_path']
    else:
        plot_path = sc['fileio']['output_path']

    if not os.path.exists(output_path+plot_path):
        os.makedirs(output_path+plot_path)

    # mirror image
    mirror_axis = []
    if mirror_xz:
        mirror_axis += ['xz']
    if mirror_xy:
        mirror_axis += ['xy']


    tilt = data.tilt.reset_index()
    tilt.loc[:, 'ts'] = pd.to_datetime(tilt['ts'])
    # mirror image
    for axis in mirror_axis:
        tilt.loc[:, axis] = -tilt[axis]
    
    max_min_df = data.max_min_df
    # mirror image
    for axis in mirror_axis:
        max_min_df.loc[:, axis+'_maxlist'] = -max_min_df[axis+'_maxlist']
        max_min_df.loc[:, axis+'_minlist'] = -max_min_df[axis+'_minlist']
        
    max_min_cml=data.max_min_cml
    # mirror image
    for axis in mirror_axis:
        max_min_cml.loc[:, axis+'_maxlist'] = -max_min_cml[axis+'_maxlist']
        max_min_cml.loc[:, axis+'_minlist'] = -max_min_cml[axis+'_minlist']

    # compute column position
    colposdf = compute_colpos(window, sc, tilt, tsm_props)

    # plot column position
    plot_column_positions(colposdf, tsm_props, window, sc, show_part_legend,
                          max_min_cml=max_min_cml)
    
    lgd = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    
    plt.savefig(output_path + plot_path + tsm_props.tsm_name + 'colpos' \
            + str(window.end.strftime('%Y-%m-%d_%H-%M'))+'.png', dpi=160, 
            facecolor='w', edgecolor='w', mode='w', bbox_extra_artists=(lgd,))
    
    # node annotation of node displacement plot
    inc_df = node_annotation(tilt, tsm_props.nos)

    # displacement plot offset
    xzd_plotoffset = plotoffset(tilt)

    # defining cumulative (surface) displacement
    cs_df = cum_surf(tilt, xzd_plotoffset, tsm_props.nos)

    #creating displacement noise envelope
    noise_df = noise_env(tilt, max_min_df, window,
                         tsm_props.nos, xzd_plotoffset)

    #zeroing and offseting xz,xy
    df0off = disp0off(tilt[['ts', 'node_id', 'xz', 'xy']], window, sc,
                      xzd_plotoffset, tsm_props.nos)

    if plotvel:
        #velplots
        if three_day_window:
            vel = tilt.loc[(tilt.ts >= window.end-timedelta(hours=3)) \
                    & (tilt.ts <= window.end)]
        else:
            vel = tilt
        #vel_xz
        vel_xz = vel[['ts', 'vel_xz', 'node_id']]
        velplot_xz,L2_xz,L3_xz = vel_classify(vel_xz, sc, tsm_props.nos)
        
        #vel_xy
        vel_xy = vel[['ts', 'vel_xy', 'node_id']]
        velplot_xy,L2_xy,L3_xy = vel_classify(vel_xy, sc, tsm_props.nos)
        
        velplot = velplot_xz, velplot_xy, L2_xz, L2_xy, L3_xz, L3_xy
    else:
        velplot = ''
    
    # plot displacement and velocity
    plot_disp_vel(noise_df, df0off, cs_df, tsm_props.tsm_name, window, sc,
                  plotvel, xzd_plotoffset, tsm_props.nos, velplot,
                  plot_inc, inc_df=inc_df)

    plt.savefig(output_path + plot_path + tsm_props.tsm_name + 'dispvel' \
            + str(window.end.strftime('%Y-%m-%d_%H-%M')) + '.png', dpi=160,
            facecolor='w', edgecolor='w', mode='w')
