from collections import Counter
from datetime import datetime, timedelta
from math import acos, cos, radians, sin, isnan
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
from numpy.linalg import lstsq
from scipy.stats import spearmanr
from sklearn import metrics
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db

#plt.ioff()
#------------------------------------------------------------------------------

def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_prop_cycle(color=[cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in
                                range(NUM_COLORS+1)[::-1]])
    return ax

def eq_events():
    query =  "SELECT eq_id, ts, magnitude AS mag, depth, "
    query += "  latitude AS lat, longitude AS lon "
    query += "FROM earthquake_events "
    query += "WHERE magnitude IS NOT NULL "
    query += "AND depth IS NOT NULL "
    query += "AND latitude IS NOT NULL "
    query += "AND longitude IS NOT NULL "
    query += "ORDER BY ts"
    eq = db.df_read(query)
    eq.loc[:, 'lat'] = eq.loc[:, 'lat'].apply(lambda x: radians(x))
    eq.loc[:, 'lon'] = eq.loc[:, 'lon'].apply(lambda x: radians(x))
    eq.loc[:, 'key'] = 1
    return eq

def site_locs():
    query =  "SELECT site_id, latitude AS lat, longitude AS lon "
    query += "FROM loggers "
    query += "WHERE site_id IN"
    query += "  (SELECT site_id FROM sites "
    query += "  WHERE active = 1) "
    query += "AND model_id != 31"
    site = db.df_read(query)

    site = site.drop_duplicates('site_id')
    site.loc[:, 'lat'] = site.loc[:, 'lat'].apply(lambda x: radians(x))
    site.loc[:, 'lon'] = site.loc[:, 'lon'].apply(lambda x: radians(x))
    site.loc[:, 'key'] = 1
    return site

def distance():
    eq = eq_events()
    site = site_locs()
    eq_site = eq.merge(site, how='outer', on='key')
    eq_site.loc[:, 'dist'] = eq_site.loc[:, ['lat_x', 'lat_y', 'lon_x', 'lon_y']].apply(lambda row: 6371.01 * acos(sin(row.lat_x)*sin(row.lat_y) + cos(row.lat_x)*cos(row.lat_y)*cos(row.lon_x - row.lon_y)), axis=1)
    tot_dist = np.sqrt(eq_site.loc[:, 'dist']**2 + eq_site.loc[:, 'depth']**2)
    eq_site.loc[:, 'tot_dist'] = tot_dist
    eq_site = eq_site.drop_duplicates(['ts', 'site_id', 'mag', 'tot_dist'])
    return eq_site.loc[:, ['site_id', 'eq_id', 'ts', 'mag', 'depth',
                    'dist', 'tot_dist']]
    
def surficial_displacement():
    query =  "SELECT site_id, marker_alerts.ts ts, alert_level FROM marker_alerts "
    query += "INNER JOIN marker_data "
    query += "USING(data_id) "
    query += "INNER JOIN marker_observations "
    query += "USING(mo_id) "
    query += "WHERE site_id IN"
    query += "  (SELECT site_id FROM sites "
    query += "  WHERE active = 1)"
    df = db.df_read(query)
    df = df.loc[df.alert_level > 0, ['site_id', 'ts', 'alert_level']]
    df.loc[:, 'alert_level'] = 1
    return df

def subsurface_movt():
    subsurface = pd.read_excel('across_axis.xlsx', sheet_name='across_axis')
    subsurface.loc[:, 'alert_level'] = subsurface.loc[:,
                  'con_mat'].map({'tp': 1, 'fp': 1, 'tn': 0, 'fn': 0})
    query = "SELECT site_id, logger_name FROM loggers"
    loggers = db.df_read(query)
    loggers_map = loggers.set_index('logger_name').to_dict()['site_id']
    subsurface.loc[:, 'site_id'] = subsurface.loc[:,
                  'tsm_name'].map(loggers_map)
    subsurface = subsurface.loc[subsurface.alert_level == 1, ['site_id', 'ts',
                                                              'alert_level']]
    return subsurface

def moms_alert():
    df = pd.read_csv('public_alert.csv')
    df = df.loc[df.trigger_type.isin(['m', 'M']), :]
    df.loc[:, 'ts'] = pd.to_datetime(df.loc[:, 'timestamp'])
    df.loc[:, 'alert_level'] = 1
    df = df.loc[:, ['site_id', 'ts', 'alert_level']]
    return df

def check_movt(eq_df, site_movt_df, days):
    index = eq_df.index.values[0]
    ts = pd.to_datetime(eq_df['ts'].values[0])
    df = site_movt_df.loc[(site_movt_df.ts >= ts) & \
                          (site_movt_df.ts <= ts + timedelta(days)), :]
    if len(df) != 0:
        eq_df.loc[eq_df.index == index, 'movt'] = 1
        eq_df.loc[eq_df.index == index, 'movt_ts'] = min(df.ts)
    else:
        eq_df.loc[eq_df.index == index, 'movt'] = 0
        eq_df.loc[eq_df.index == index, 'movt_ts'] = np.nan
    return eq_df

def check_eq_movt_corr(eq_df, movt_df, days):
    site_id = eq_df['site_id'].values[0]
    print('site #', site_id)
    site_movt_df = movt_df.loc[movt_df.site_id == site_id, :]
    per_eq_id = eq_df.groupby('eq_id', as_index=False)
    eq_df = per_eq_id.apply(check_movt, site_movt_df=site_movt_df,
                            days=days)
    return eq_df.reset_index(drop=True)

def confusion_matrix(matrix, eq_df):
    index = matrix.index[0]
    crit_dist = matrix['crit_dist'].values[0]
    eq_df.loc[eq_df.dist <= crit_dist, 'pred'] = 1
    eq_df.loc[eq_df.dist > crit_dist, 'pred'] = 0
    matrix.loc[matrix.index == index,
               'tp'] = len(eq_df.loc[(eq_df.movt == 1) & (eq_df.pred == 1), :])
    matrix.loc[matrix.index == index,
               'tn'] = len(eq_df.loc[(eq_df.movt == 0) & (eq_df.pred == 0), :])
    matrix.loc[matrix.index == index,
               'fp'] = len(eq_df.loc[(eq_df.movt == 0) & (eq_df.pred == 1), :])
    matrix.loc[matrix.index == index,
               'fn'] = len(eq_df.loc[(eq_df.movt == 1) & (eq_df.pred == 0), :])
    return matrix

def movt_per_mag(eq_df, ax):
    mag = eq_df['mag'].values[0]
    matrix = pd.DataFrame({'crit_dist': np.arange(0.5, 1000.5, 0.5)})
    matrix.loc[:, 'mag'] = mag
    dist_matrix = matrix.groupby('crit_dist', as_index=False)
    matrix = dist_matrix.apply(confusion_matrix,
                               eq_df=eq_df).reset_index(drop=True)
    matrix.loc[:, 'tpr'] = matrix.loc[:, 'tp'] / (matrix.loc[:, 'tp'] + 
                                                  matrix.loc[:, 'fn'])
    matrix.loc[:, 'fpr'] = matrix.loc[:, 'fp'] / (matrix.loc[:, 'fp'] + 
                                                  matrix.loc[:, 'tn'])
    matrix = matrix.sort_values(['fpr', 'tpr'])
    matrix = matrix.fillna(0)
    ax.plot(matrix['fpr'].values, matrix['tpr'].values, '.-', label=mag)
    return matrix

def rainfall_alert():
    pub = pd.read_csv('public_alert.csv')
    rainfall = pub.loc[(pub.trigger_type == 'R') &
                       (pub.timestamp >= '2017-10-10 20:15'),
                       ['site_id', 'timestamp']]
    rainfall.loc[:, 'ts'] = rainfall.loc[:, 'timestamp'].apply(lambda x: 
                                                               pd.to_datetime(x))
    return rainfall

def rainfall_corr(eq_movt_df, rain_df):
    site_id = eq_movt_df['site_id'].values[0]
    movt_ts = pd.to_datetime(eq_movt_df['movt_ts'].values[0])
    with_rain = rain_df.loc[(rain_df.site_id == site_id) &
                            (rain_df.ts <= movt_ts) & 
                            (rain_df.ts >= movt_ts - timedelta(3)), :]
    if len(with_rain) != 0:
        return eq_movt_df['index'].values[0]

def eq_movt_corr(days, recompute=True, mag_filter=True, rain_filter=True):
    if recompute:
        # earthquake
        eq_dist = distance()
        eq_dist = eq_dist.loc[(eq_dist.ts >= '2017-10-10 20:15') & \
                              (eq_dist.ts <= '2019-01-19 03:00'), :]
        # ground movement
        surficial = surficial_displacement()
        subsurface = subsurface_movt()
        moms = moms_alert()
        movt_df = surficial.append(subsurface).append(moms)
        movt_df = movt_df.loc[(movt_df.ts >= '2017-10-13 20:15') & \
                              (movt_df.ts <= '2019-01-22 03:00'), :]
        site_eq_dist = eq_dist.groupby('site_id', as_index=False)
        eq_movt = site_eq_dist.apply(check_eq_movt_corr, movt_df=movt_df,
                                     days=days).reset_index(drop=True)
        eq_movt.to_csv('data\\eq_movt' + str(np.round(float(days), 2)) + '.csv',
                       index=False)
    else:
        eq_movt = pd.read_csv('data\\eq_movt' + str(np.round(float(days),
                                                             2)) + '.csv')
    
    # data filter
    if mag_filter:
        mag_count = Counter(eq_movt.mag)
        mag_list = list(k for k, v in mag_count.items() if v >= 500)
        eq_movt = eq_movt.loc[eq_movt.mag.isin(mag_list), :]
    if rain_filter:
        rainfall = rainfall_alert()
        eq_pos_movt = eq_movt.loc[eq_movt.movt == 1, :].reset_index()
        grp_eq_movt = eq_pos_movt.groupby('index', as_index=False)
        eq_movt_index = grp_eq_movt.apply(rainfall_corr, rain_df=rainfall)
        eq_movt.loc[~eq_movt.index.isin(eq_movt_index), :]
    return eq_movt

def roc(days, recompute=True, by_sklearn=True, mag_filter=True,
         rain_filter=True, include_depth=True):
    
    eq_movt = eq_movt_corr(days, recompute=recompute, mag_filter=mag_filter,
                           rain_filter=rain_filter)
    
    if include_depth:
        site_dist = 'tot_dist'
    else:
        site_dist = 'dist'
        
    all_matrix = pd.DataFrame()
    
    for i in range(1,7):
        if i != 6:
            seg_eq_movt = eq_movt.loc[(eq_movt.mag > i) & (eq_movt.mag <= i+1), :]
            filename = 'ROC mag ' + str(i) + ' to ' + str(i+1)
        else:
            seg_eq_movt = eq_movt.loc[eq_movt.mag > i, :]
            filename = 'ROC mag greater than ' + str(i)
        
        if len(seg_eq_movt) != 0:
            fig = plt.figure(figsize=(10,10))
            ax = fig.add_subplot(111)
            num_colors = len(set(seg_eq_movt.mag))
            ax = nonrepeat_colors(ax, num_colors, color='plasma')
            
            for mag in sorted(set(seg_eq_movt.mag)):
                eq_mag_movt = seg_eq_movt.loc[seg_eq_movt.mag == mag, :]
                if by_sklearn:
                    y = eq_mag_movt.movt
                    scores = eq_mag_movt[site_dist].values
                    fpr, tpr, thresholds = metrics.roc_curve(y, scores)
                    yi = tpr - fpr
                    matrix = pd.DataFrame({'crit_dist': thresholds, 'tpr': tpr,
                                           'fpr': fpr, 'yi': yi})
                    optimal = matrix.loc[matrix.yi == max(yi), :]
                    label = 'mag ' + str(mag)
                    if len(optimal) != 0:
                        optimal_dist = optimal['crit_dist'].values[0]
                        label += ': ' + str(round(optimal_dist, 2)) + ' km'
                    ax.plot(fpr, tpr, '.-', label=label)
                    if len(optimal) != 0:
                        ax.plot(optimal['fpr'].values[0],
                                optimal['tpr'].values[0], 'ko')
                    matrix.loc[:, 'mag'] = mag
                else:
                    matrix = movt_per_mag(eq_mag_movt, ax)
                all_matrix = all_matrix.append(matrix)
                
            ax.plot([0, 1], [0, 1], 'k--')
            ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
            ax.set_xlabel('FPR', fontsize = 14)
            ax.set_ylabel('TPR', fontsize = 14)
            plt.title(str(days) + 'D')
            plt.savefig(filename + ' D' + str(days) + '.png', facecolor='w',
                        edgecolor='w', mode='w', bbox_inches = 'tight')
            plt.close()
        
    return all_matrix


def optimal_threshold(matrix):
    yi = matrix['tpr'] - matrix['fpr']
    optimal = matrix.loc[(matrix.yi == max(yi)) & (matrix.yi >= 0), :]
    if len(optimal) != 0:
        mag_threshold = pd.DataFrame({'mag': [optimal['mag'].values[0]],
                                      'crit_dist': [min(optimal.crit_dist)]})
        return mag_threshold

def plot_optimal(day_list, crit_dist_limit=500, plot_indiv=True,
                 plot_stack=True):
    matrix_dict = {}
    optimal_dict = {}
    for days in day_list:
        matrix_dict[days] = roc(1, recompute=False)
        grp_matrix = matrix_dict.get(days).groupby('mag', as_index=False)
        optimal = grp_matrix.apply(optimal_threshold)
        optimal_dict[days] = optimal.loc[optimal.crit_dist < 1000, :]
        if plot_indiv:
            fig = plt.figure()
            ax = fig.add_subplot(111)
            ax.plot(optimal.mag, optimal.crit_dist, '.-', label='1D')
            ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
            ax.set_xlabel('magnitude', fontsize = 14)
            ax.set_ylabel('critical distance', fontsize = 14)
            plt.savefig('optimal_threshold' + ' D' + str(days) + '.png',
                        facecolor='w', edgecolor='w', mode='w',
                        bbox_inches = 'tight')
    if plot_stack:
        fig = plt.figure()
        ax = fig.add_subplot(111)
        num_colors = len(day_list)
        ax = nonrepeat_colors(ax, num_colors, color='plasma')
        for days in day_list:
            optimal = optimal_dict.get(days)
            ax.plot(optimal.mag, optimal.crit_dist, '.-', label='1D')
        ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
        ax.set_xlabel('magnitude', fontsize = 14)
        ax.set_ylabel('critical distance', fontsize = 14)
        plt.savefig('optimal_threshold.png', facecolor='w', edgecolor='w',
                    mode='w', bbox_inches = 'tight')

def plot_eq_movt_dist(days, depth_min, depth_max, step_size, plot_stack = True,
                      plot_indiv=True, mag_filter=True, recompute=True):

    eq_movt = eq_movt_corr(days, mag_filter=mag_filter, recompute=recompute)
    with_movt = eq_movt.loc[(eq_movt.movt == 1), :]
    without_movt = eq_movt.loc[(eq_movt.movt == 0), :]
    
    if plot_stack:
        fig = plt.figure()
        ax = fig.add_subplot(111)
        num_colors = len(set(with_movt.mag))
        ax.plot(without_movt.mag, without_movt.tot_dist, '.', color='#b3b3b3')
        ax = nonrepeat_colors(ax, num_colors, color='plasma')
        for mag in sorted(set(with_movt.mag), reverse=True):
            mag_with_movt = with_movt.loc[with_movt.mag == mag, :]
            ax.plot(mag_with_movt.mag, mag_with_movt.tot_dist, '.',
                    markersize=12)
        ax.set_ylim(0,2000)
        ax.set_facecolor('#383838')
        ax.set_xlabel('magnitude', fontsize = 14)
        ax.set_ylabel('critical distance', fontsize = 14)
        plt.title('days: ' + str(np.round(days, 2)))
        plt.savefig('critdist' + str(np.round(days, 2)) + '.png', facecolor='w',
                    edgecolor='w', mode='w', bbox_inches = 'tight')
    
    if plot_indiv:
        for depth in np.arange(depth_min, depth_max, step_size):
            d_min = depth
            d_max = depth + step_size
            
            depth_with_movt = with_movt.loc[(with_movt.depth >= d_min)
                                          & (with_movt.depth <= d_max), :]
            depth_without_movt = without_movt.loc[(without_movt.depth >= d_min) &
                                       (without_movt.depth <= d_max), :]
            
            if len(depth_with_movt) != 0:
                fig = plt.figure()
                ax = fig.add_subplot(111)
                num_colors = len(set(depth_with_movt.mag))
                ax.plot(depth_without_movt.mag, depth_without_movt.tot_dist,
                        '.', color='#b3b3b3')
                ax = nonrepeat_colors(ax, num_colors, color='plasma')
                for mag in sorted(set(depth_with_movt.mag), reverse=True):
                    mag_with_movt = depth_with_movt.loc[depth_with_movt.mag == mag, :]
                    ax.plot(mag_with_movt.mag, mag_with_movt.tot_dist, '.',
                            markersize=12)
                ax.set_ylim(0,2000)
                ax.set_facecolor('#383838')
                ax.set_xlabel('magnitude', fontsize = 14)
                ax.set_ylabel('critical distance', fontsize = 14)
                plt.title('depth: ' + str(d_min) + ' to ' + str(d_max))
                plt.savefig('critdist_depth' + str(d_min) + 'to' + str(d_max) + '.png',
                            facecolor='w', edgecolor='w', mode='w',
                            bbox_inches = 'tight')

def plot_eq_movt_depth(days, depth_min, depth_max, step_size, plot_stack = True,
                      plot_indiv=True, mag_filter=True, recompute=True):

    eq_movt = eq_movt_corr(days, mag_filter=mag_filter, recompute=recompute)
    with_movt = eq_movt.loc[(eq_movt.movt == 1), :]
    without_movt = eq_movt.loc[(eq_movt.movt == 0), :]
    
    if plot_stack:
        fig = plt.figure()
        ax = fig.add_subplot(111)
        num_colors = len(set(with_movt.mag))
        ax.plot(without_movt.mag, without_movt.depth, '.', color='#b3b3b3')
        ax = nonrepeat_colors(ax, num_colors, color='plasma')
        for mag in sorted(set(with_movt.mag), reverse=True):
            mag_with_movt = with_movt.loc[with_movt.mag == mag, :]
            ax.plot(mag_with_movt.mag, mag_with_movt.depth, '.',
                    markersize=12)
        ax.set_ylim(0,2000)
        ax.set_facecolor('#383838')
        ax.set_xlabel('magnitude', fontsize = 14)
        ax.set_ylabel('depth', fontsize = 14)
        plt.title('days: ' + str(np.round(days, 2)))
        plt.savefig('depth' + str(np.round(days, 2)) + '.png', facecolor='w',
                    edgecolor='w', mode='w', bbox_inches = 'tight')


def prob_LgivenE(days, mag_bin_step=0.5, dist_bin_step=25, dist_limit=250,
                 mag_filter=True, recompute=True):

    eq_movt = eq_movt_corr(days, mag_filter=mag_filter, recompute=recompute)
    eq_movt = eq_movt.loc[eq_movt.tot_dist <= dist_limit]

    # Construct arrays for the anchor positions of the bars
    mag_bin = np.arange(2, 6, mag_bin_step)
    dist_bin = np.arange(0, dist_limit, dist_bin_step)
    xpos, ypos = np.meshgrid(dist_bin[:-1] + dist_bin_step/2,
                             mag_bin[:-1] + mag_bin_step/2, indexing="ij")
    xpos = xpos.ravel()
    ypos = ypos.ravel()
    zpos = 0
    # Construct arrays with the dimensions for the bars.
    dx = dist_bin_step/2
    dy = mag_bin_step/2

    prob_df = pd.DataFrame()
    
    site_pga = pd.read_csv('data\\site_pga.csv')
    pga_dist = site_pga.pga.value_counts()
    for pga in sorted(pga_dist[pga_dist > 1].keys()):
        site_list = site_pga.loc[site_pga.pga >= pga, 'site_id']
        with_movt = eq_movt.loc[(eq_movt.movt == 1) & 
                                (eq_movt.site_id.isin(site_list)), :]
        without_movt = eq_movt.loc[(eq_movt.movt == 0) & 
                                   (eq_movt.site_id.isin(site_list)), :]
        if len(with_movt) <= 1:
            continue
        with_hist, xedges, yedges = np.histogram2d(with_movt.tot_dist,
                                                   with_movt.mag,
                                                   bins=[dist_bin, mag_bin])
        without_hist, xedges, yedges = np.histogram2d(without_movt.tot_dist,
                                                      without_movt.mag,
                                                      bins=[dist_bin, mag_bin])
        prob = with_hist / (with_hist + without_hist)
        prob = np.where(prob==0, np.nan, prob)
        dz = prob.ravel()
        log_dz = np.log10(dz*10000)
        
        if isnan(np.nanmax(prob)):
            continue
        
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')

        ax.bar3d(xpos, ypos, zpos, dx, dy, dz, zsort='average')
        
        zz = log_dz[~np.isnan(dz)]
        xx = (xpos.ravel()+dx)[~np.isnan(dz)]
        yy = (ypos.ravel()+dy)[~np.isnan(dz)]
        ones = [ [1] * len( xx.flatten() )]
        indep = np.column_stack( [xx.flatten(), yy.flatten()] + ones )
        model = lstsq(indep, zz.flatten(), rcond=-1)[0]
        x_vals = np.arange(0, 250, 0.5)
        y_vals = np.arange(1, 5, 0.1)
        xx_vals, yy_vals = np.meshgrid(x_vals, y_vals)
        zz_vals = (10 ** (model[0] * xx_vals + model[1] * yy_vals + model[2])) / 10000
        ax.plot_surface(xx_vals, yy_vals, zz_vals, cmap=cm.coolwarm, linewidth=0, alpha=0.5)

        ax.set_zlim3d(0 ,np.nanmax((list(zz_vals.flatten())+list(dz.flatten()))).flatten())
        ax.set_title('PGA = {}g\nDuration = {}H'.format(pga,
                     int(days*24)))
        ax.set_xlabel('Hypocentral Distance (km)')
        ax.set_ylabel('Magnitude')
        plt.savefig("pga\\{}g{}H.png".format(pga, int(days*24)), facecolor='w',
                    edgecolor='w', mode='w', bbox_inches = 'tight')
        plt.close()

        try:
            prob_mag = prob[list(map(lambda x: not all(np.isnan(x)), prob))]
            prob_dist = prob.T[list(map(lambda x: not all(np.isnan(x)), prob.T))]
            prob_mag = list(map(lambda x: spearmanr(range(len(x)), x,
                                                     nan_policy='omit')[0], 
                                prob_mag))
            prob_dist = list(map(lambda x: -spearmanr(range(len(x)), x,
                                                      nan_policy='omit')[0],
                                 prob_dist))
            prob_df = prob_df.append(pd.DataFrame({'hours': [int(days*24)],
                                                   'pga': [pga],
                                                   'prob_mag': [prob_mag],
                                                   'prob_dist': [prob_dist]}),
                                     ignore_index=True)
        except:
            pass
    
    return prob_df

###############################################################################

if __name__ == '__main__':
    start_time = datetime.now()
    
    prob_df = pd.DataFrame()
    time_range = [1, 2, 3, 4, 8, 12, 24, 48, 72]
    for numerator in time_range:
        curr_prob = prob_LgivenE(numerator/24, recompute=True, mag_filter=False,
                                 mag_bin_step=0.5, dist_bin_step=25)
        prob_df = prob_df.append(curr_prob, ignore_index=False)
        
    prob_df.loc[:, 'dist'] = prob_df.loc[:, 'prob_dist'].apply(lambda x: np.sum(x))
    prob_df.loc[:, 'mag'] = prob_df.loc[:, 'prob_mag'].apply(lambda x: np.sum(x))
    prob_df.loc[:, 'prob'] = prob_df.loc[:, 'dist'] + prob_df.loc[:, 'mag']    

    print('runtime =', datetime.now() - start_time)