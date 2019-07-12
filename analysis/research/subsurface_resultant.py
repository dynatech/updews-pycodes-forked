import pandas as pd
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
#import querydb as qdb
#from IPython.display import display
from numpy.lib.stride_tricks import as_strided
from numpy.lib import pad
import filterdata as filt
import analysis.querydb as qdb
import proc
from proc import no_initial_data
from proc import get_last_good_data
from collections import OrderedDict
import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
import seaborn as sns
import erroranalysis as err
import rtwindow as rtw
from pyfinance.ols import PandasRollingOLS
from datetime import datetime, date, time, timedelta


class ProcData:
    def __init__ (self, invalid_nodes, tilt, lgd, max_min_df, max_min_cml):
        self.inv = invalid_nodes
        self.tilt = tilt
        self.lgd = lgd
        self.max_min_df = max_min_df
        self.max_min_cml = max_min_cml

def no_data(df, num_nodes):
    allnodes = np.arange(1, num_nodes+1)
    withval = sorted(set(df.node_id))
    noval = allnodes[np.in1d(allnodes, withval, invert=True)]
    return noval

def magnitude(df,seg_len):
    df['magnitude'] = np.round((np.sqrt((df['x'] - df.iloc[0].x) ** 2 + (df['y'] - df.iloc[0].y) ** 2 + (df['z'] - df.iloc[0].z) ** 2) * seg_len / 1024), 4)
    
    return df

def theta_yz(df):
    df['theta_yz'] = np.arccos(df['y']/(np.sqrt(df['y']** 2 + df['z']** 2)))
    
    return(df)
    
def fill_smooth(df, offsetstart, end, roll_window_numpts, to_smooth, to_fill):    
    if to_fill:
        # filling NAN values
        df = df.fillna(method = 'pad')
        
        #Checking, resolving and reporting fill process    
        if df.isnull().values.any():
            for n in ['magnitude', 'theta_yz']:
                if df[n].isnull().values.all():
#                    node NaN all values
                    df.loc[:, n] = 0
                elif np.isnan(df[n].values[0]):
#                    node NaN 1st value
                    df.loc[:, n] = df[n].fillna(method='bfill')

    #dropping rows outside monitoring window
    df=df[(df.index >= offsetstart) & (df.index <= end)]
    
    if to_smooth and len(df)>1:
        df.loc[:, ['magnitude', 'theta_yz']] = df[['magnitude', 'theta_yz']].rolling(window=roll_window_numpts, min_periods=1).mean()
        df = np.round(df[roll_window_numpts-1:], 4)
           
    return df

def rolling_spearman(df, seqa, seqb, window):
    stridea = seqa.strides[0]
    ssa = as_strided(seqa, shape=[len(seqa) - window + 1, window], strides=[stridea, stridea])
    strideb = seqa.strides[0]
    ssb = as_strided(seqb, shape=[len(seqb) - window + 1, window], strides =[strideb, strideb])
    ar = pd.DataFrame(ssa)
    br = pd.DataFrame(ssb)
    ar = ar.rank(1)
    br = br.rank(1)
    corrs = ar.corrwith(br, 1)
    df['spearman'] = pad(corrs, (window - 1, 0), 'constant', constant_values=np.nan)
    
    return(df['spearman'])

def slope_intercept(df, roll_window_numpts, start):
    
    slope = PandasRollingOLS(y=df.magnitude, x=df.td, window=7).beta
    intercept = PandasRollingOLS(y=df.magnitude, x=df.td, window=7).alpha
    
    inter = pd.DataFrame(intercept, columns=['intercept'])

    df = df.loc[df.index >= start]

    m = slope[slope.index >= start]['feature1'].values
    df.loc[:, 'slope'] = np.round(m, 4)
    
    b = inter[inter.index >= start]['intercept'].values
    df.loc[:, 'intercept'] = np.round(b, 4)
    
    
    return(df)


def displacement(df):
    df['linreg'] = (df['slope'] * df['td']) + df['intercept']
#    df['MA'] = df['linreg'].rolling(7).mean()
    df['displacement'] = np.round(df['linreg'] - df.shift(144)['linreg'], 4)
    df['beta'] = np.arccos(df['x'] / np.sqrt(df['x'] ** 2 + df['y'] ** 2 + df['z'] ** 2))
    df['beta_trend'] = rolling_spearman(df, df.td, df.beta, 144)
    df = df.drop(columns=['spearman'])
    df['yz_trend'] = rolling_spearman(df, df.td, df.theta_yz, 144)
    df = df.drop(columns=['spearman'])
    
    return(df)
    
def node_inst_vel(df, roll_window_numpts, start):
    lr_xyz = PandasRollingOLS(y=df.magnitude, x=df.td, window=7).beta
    
    df = df.loc[df.index >= start + timedelta(hours=3.5)]

    velocity = lr_xyz[lr_xyz.index >= start + timedelta(hours=3.5)]['feature1'].values
    df.loc[:, 'velocity'] = np.round(velocity, 4)
    
    return(df)

def accel(df, roll_window_numpts, start):
    accel = PandasRollingOLS(y=df.velocity, x=df.td, window=144).beta

    df = df.loc[df.index >= start + timedelta(hours=75.5)]

    acceleration = accel[accel.index >= start + timedelta(hours=75.5)]['feature1'].values
    df.loc[:, 'acceleration'] = np.round(acceleration, 4)
    
    return(df)

def is_accelerating(df, threshold, spearman):
    '''for velocity'''
#    df['actual'] = np.where((df['acceleration']> 0) & (abs(df['beta_trend']) > 0.6) | (abs(df['yz_trend']) > 0.6), 'p', 'n')
#    df['predicted'] = np.where((abs(df['velocity']) > threshold) & (abs(df['beta_trend']) > 0.6) | (abs(df['yz_trend']) > 0.6), 'p', 'n')
    
    '''for displacement'''
    df['actual'] = np.where((df['acceleration'] > 0) & (abs(df['beta_trend']) > spearman) | (abs(df['yz_trend']) > spearman), 'p', 'n')
    df['predicted'] = np.where((abs(df['velocity']) > threshold) & (abs(df['beta_trend']) > spearman) | (abs(df['yz_trend']) > spearman), 'p', 'n')
    df['threshold'] = threshold
    df['spearman'] = spearman
    
    return(df)

def confusion_matrix(df):
    df['con_mat'] = np.where((df['actual'] == 'p') & (df['predicted'] == 'p'), 'true positive', 'none')
    df['con_mat'] = np.where((df['actual'] == 'n') & (df['predicted'] == 'n'), 'true negative', df['con_mat'])
    df['con_mat'] = np.where((df['actual'] == 'p') & (df['predicted'] == 'n'), 'false negative', df['con_mat'])
    df['con_mat'] = np.where((df['actual'] == 'n') & (df['predicted'] == 'p'), 'false positive', df['con_mat'])
    
    return(df)
    
def roc(df):
    df['tp'] = len(df.loc[df['con_mat'] == 'true positive'])
    df['tn'] = len(df.loc[df['con_mat'] == 'true negative'])
    df['fp'] = len(df.loc[df['con_mat'] == 'false positive'])
    df['fn'] = len(df.loc[df['con_mat'] == 'false negative'])
    
    df['p'] = df['tp']+df['fn']
    df['n'] = df['fp']+df['tn']
    
#    tp_rate = tp/p
#    fp_rate = fp/n
    
#    specificity = (tn) / (fp+tn)
#    sensitivity = tp_rate
#    
#    prevalence = (tp + fn) / (tp + fn + tn+fp)
#    
#    precision = tp/(tp+fp)
#    adjusted_ppv = (sensitivity * prevalence) / (sensitivity * prevalence) + ((1-specificity) * (1-prevalence))
#    youden_index = (sensitivity + specificity) - 1
#    ss = sensitivity + specificity
#    dist_to_corner = np.sqrt((1-sensitivity)**2 + (1-specificity)**2)
#    
    
#    roc_point = (fp_rate, tp_rate, threshold, precision, adjusted_ppv, youden_index, ss, dist_to_corner)
#    roc_point = pd.DataFrame(columns=['tp','fp', 'tn', 'fn', 'p','n','threshold'])
#    roc_point = (tp, fp, tn, fn, p, n, threshold)
    
    return(df)

def finalize_roc(roc_df):
    tp = roc_df['tp'].sum() 
    fp = roc_df['fp'].sum()
    tn = roc_df['tn'].sum()
    fn = roc_df['fn'].sum() 
    p = roc_df['p'].sum()
    n  = roc_df['n'].sum()
    specificity = (tn) / (fp+tn)
    
    tp_rate = tp/p
    fp_rate = fp/n
    
    mcc_n =  (tp*tn)-(fp*fn)
    mcc_d_1 = np.multiply((tp+fp), (tp+fn))
    mcc_d_2 = np.multiply((tn+fp), (tn+fn))
    mcc_d = np.sqrt(np.multiply(mcc_d_1, mcc_d_2))
    mcc = mcc_n / mcc_d
    f1_score = (2*tp)/((2*tp) + fp + fn)
    dist_to_corner = np.sqrt((1-tp_rate)**2 + (1-specificity)**2)
    accuracy = (tn+tp) / (tn+tp+fn+fp)
    
    return(tp, fp, tn, fn, fp_rate, tp_rate, roc_df.threshold[0], roc_df.spearman[0], mcc, f1_score, dist_to_corner,accuracy)
#    return(fp_rate, tp_rate, roc_df.threshold[0])


def alert_generator(df, disp_threshold, vel_threshold):
    df['disp_alert'] = np.where((abs(df['displacement']) >disp_threshold) & ((abs(df['beta_trend']) > 0.6) | (abs(df['yz_trend']) > 0.6)) > disp_threshold, 2, 0)
    df['vel_alert'] = np.where((abs(df['velocity']) > vel_threshold) & ((abs(df['beta_trend']) > 0.6) | (abs(df['yz_trend']) > 0.6)), 2, 0)
#    df['alert'] = np.where(abs(df['velocity'] > 0.5), 'L2 velocirocty', df['alert'])

    return(df)
    
def takeSecond(elem):
    return elem[0]

def nonrepeat_colors(ax,NUM_COLORS,color='plasma'):
   cm = plt.get_cmap(color)
   ax.set_prop_cycle(color=[cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in
                               range(NUM_COLORS+1)[::-1]])
   return ax

def proc_subsurface(tsm_props, window, sc):
    monitoring = qdb.get_raw_accel_data(tsm_name= tsm_props.tsm_name, from_time=window.offsetstart, to_time=window.end)

    monitoring = monitoring.loc[monitoring.node_id <= tsm_props.nos]
    monitoring = filt.apply_filters(monitoring)
    monitoring = monitoring.groupby('node_id', as_index=False).apply(magnitude, tsm_props.seglen)
    monitoring = theta_yz(monitoring)
    
    #identify the node ids with no data at start of monitoring window
    no_init_val = no_initial_data(monitoring, tsm_props.nos, window.offsetstart)
    
    #get last good data prior to the monitoring window (LGDPM)
    if len(no_init_val) != 0:
        lgdpm = qdb.get_single_lgdpm(tsm_props.tsm_name, no_init_val, window.offsetstart)
        lgdpm = filt.apply_filters(lgdpm)
        lgdpm = lgdpm.sort_index(ascending = False).drop_duplicates('node_id')
        
        monitoring=monitoring.append(lgdpm, sort=False)
    
    invalid_nodes = qdb.get_node_status(tsm_props.tsm_id, ts=window.offsetstart)
    monitoring = monitoring.loc[~monitoring.node_id.isin(invalid_nodes)]
    
    lgd = get_last_good_data(monitoring)
    
    #assigns timestamps from LGD to be timestamp of offsetstart
    monitoring.loc[(monitoring.ts<window.offsetstart)|(pd.isnull(monitoring.ts)),
                   ['ts']] = window.offsetstart
    
               
    monitoring = monitoring.drop_duplicates(['ts', 'node_id'])
    monitoring = monitoring.set_index('ts')
    #monitoring = monitoring.loc[monitoring['node_id'].isin(range(1,8,1))] select nodes
    
    monitoring = monitoring[['tsm_name', 'node_id', 'x', 'y', 'z', 'magnitude', 'theta_yz']]
    
    nodes_noval = no_data(monitoring, tsm_props.nos)
    nodes_nodata = pd.DataFrame({'tsm_name': [tsm_props.tsm_name]*len(nodes_noval),
                        'node_id': nodes_noval, 'magnitude': [np.nan]*len(nodes_noval),
                        'theta_yz': [np.nan]*len(nodes_noval),
                         'ts': [window.offsetstart]*len(nodes_noval)})
    nodes_nodata = nodes_nodata.set_index('ts')
    monitoring = monitoring.append(nodes_nodata, sort=False)
    
#    print ('\n\n\n####### monitoring #######\n\n\n', monitoring)
    
    max_min_df, max_min_cml = err.cml_noise_profiling(monitoring, sc, tsm_props.nos)
    
    monitoring = monitoring.groupby('node_id', as_index=False).apply(proc.resample_node,
                         window = window).reset_index(drop=True).set_index('ts')
    
    to_smooth = int(sc['subsurface']['to_smooth'])
    to_fill = int(sc['subsurface']['to_fill'])
    
    monitoring = monitoring.groupby('node_id', as_index=False).apply(fill_smooth,
                         offsetstart=window.offsetstart, end=window.end,
                         roll_window_numpts=window.numpts, to_smooth=to_smooth,
                         to_fill=to_fill).reset_index(level='ts').set_index('ts')
    
    
    monitoring.loc[:, 'td'] = monitoring.index.values - monitoring.index.values[0]
    monitoring.loc[:, 'td'] = monitoring['td'].apply(lambda x: x / \
                                            np.timedelta64(1,'D'))

    monitoring = monitoring.groupby('node_id', as_index=False).apply(slope_intercept,
                                            roll_window_numpts=window.numpts,
                                            start=window.start).reset_index(level='ts').set_index('ts')
    monitoring = displacement(monitoring)
    
    monitoring = monitoring.groupby('node_id', as_index=False).apply(node_inst_vel,
                                            roll_window_numpts=window.numpts,
                                            start=window.start).reset_index(level='ts').set_index('ts')

#    monitoring = monitoring.groupby('node_id', as_index=False).apply(accel,
#                                            roll_window_numpts=window.numpts,
#                                            start=window.start).reset_index(level='ts').set_index('ts')
    
    tilt = alert_generator(monitoring, 0.05, 0.032)
    
    return ProcData(invalid_nodes,tilt,lgd,max_min_df,max_min_cml)


#tsm_name = 'magta'
#window, sc = rtw.get_window('2017-07-01 00:00:00', 300)
#tsm_props = qdb.get_tsm_list(tsm_name)[0]
#sub_data = proc_subsurface(tsm_props, window=window, sc=sc)
#x = np.arange(0.01, 0.2, 0.01)
#y = np.arange(0.01, 0.2, 0.01)
#roc_points = []
#for i in range(0, len(x)):
#    threshold = np.round(x[i], 4)
#    for j in range(0, len(y)):
#        spearman = np.round(y[j], 4)
#        roc_df = alert_generator(monitoring, threshold, 0)
#        roc_df = is_accelerating(monitoring, threshold, spearman)
#        roc_df = confusion_matrix(monitoring)
#        roc_df = roc_df.groupby('node_id', as_index=False).apply(roc)
#        roc_df = roc_df.drop_duplicates(['node_id'])
#        roc_df = roc_df.groupby('threshold', as_index=False).apply(finalize_roc)
#        print(roc_df[0])
#        roc_points.append(roc_df[0])
#
#roc_points= pd.DataFrame(roc_points, columns=['tp', 'fp', 'tn', 'fn', 'fpr', 'tpr', 'threshold', 'spearman', 'mcc', 'f1_score', 'dist', 'accuracy'])
#roc_points = roc_points.dropna()
#xs = roc_points.fpr
#ys = roc_points.tpr
#roc_points.sort_values('fpr')
#fig = plt.figure()
#ax1 = fig.add_subplot(111)
#for k in range(0, len(roc_points)-1):
#    p = roc_points.loc[roc_points['spearman'] == roc_points.iloc[k].spearman]
#    p.loc[-1] = [0,0,0,0,0,0,0,0,0,0,0,0]
#    p.index = p.index + 1
#    p.loc[len(roc_points)] = [0,0,0,0,1,1,0,0,0,0,0,0]
#    p = p.sort_values('fpr')
##    ax1 = nonrepeat_colors(ax1, len(p))
#    plt.plot(p.fpr, p.tpr, marker ='o', label=str(p.iloc[1].spearman))
#
#handles, labels = plt.gca().get_legend_handles_labels()
#by_label = OrderedDict(zip(labels, handles))
#plt.legend(by_label.values(), by_label.keys())


#roc_points = pd.DataFrame(roc_points, columns=['x', 'y', 'threshold'])

#roc_points= pd.DataFrame(roc_points, columns=['fpr', 'tpr', 'threshold'])
#roc_points= pd.DataFrame(roc_points, columns=['tp', 'fp', 'tn', 'fn', 'fpr', 'tpr', 'threshold', 'spearman', 'mcc', 'f1_score', 'dist', 'accuracy'])
#roc_points = roc_points.dropna().reset_index()
#xs = roc_points.fpr
#ys = roc_points.tpr

#fig = plt.figure()    
#with sns.axes_style("ticks"):
#    sns.set_palette(sns.color_palette("hls", len(x)))

#ax1 = fig.add_subplot(511)
#ax1 = nonrepeat_colors(ax1, len(x))
#ax1.set_ylabel('$True\ Positive\ Rate$', fontsize = 15)
#ax1.set_xlabel('$False\ Positive\ Rate$', fontsize = 15)
#ax1.plot(xs, ys, '-', linewidth = 1, color = 'brown')
#
#for i in range(0, len(roc_points)-1):
#    ax1.scatter(xs[i], ys[i], label = str(roc_points.threshold[i]))
#
##roc_points.plot('fpr', 'tpr', kind='scatter')
##roc_points[['fpr','tpr','threshold']].apply(lambda fpr: ax8.text(*fpr),axis=1)
#
#
#ax1.set_xticks(np.arange(0, 1.1, 0.1))
#ax1.set_yticks(np.arange(0, 1.1, 0.1))
#ax1.plot([0, 1], [0, 1],  linewidth = 2, linestyle = '--', color = 'royalblue')
#
#
#fig.suptitle('$ROC\ Curve\ of\ Displacement\ (Threshold\ range\ .5cm\ to\ 5cm,\ %s\ all nodes\ 1 year data)$' %tsm_name, fontsize = 20)
#box = ax1.get_position()
#ax1.set_position([box.x0, box.y0, box.width * 0.9, box.height])

# Put a legend to the right of the current axis
#ax8.legend(loc='upper left', bbox_to_anchor=(1, 0.85))
#sns.despine()
#ax1.plot('threshold', 'mcc', data=roc_points)
#
#ax2 = fig.add_subplot(512)
#ax2.plot('threshold', 'tp', data=roc_points)
#
#ax3 = fig.add_subplot(513)
#ax3.plot('threshold', 'fp', data=roc_points)
##
#ax4 = fig.add_subplot(514)
#ax4.plot('threshold', 'tn', data=roc_points)
##
#ax5 = fig.add_subplot(515)
#ax5.plot('threshold', 'tn', data=roc_points)


#plt.plot(x,y)
#
#
#plt.show()
