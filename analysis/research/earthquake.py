from collections import Counter
from datetime import datetime, timedelta
from sklearn import metrics
import math
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
#------------------------------------------------------------------------------

def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_prop_cycle(color=[cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in
                                range(NUM_COLORS+1)[::-1]])
    return ax

def eq_events():
    query =  "SELECT eq_id, ts, magnitude, depth, latitude, longitude "
    query += "FROM earthquake_events "
    query += "WHERE magnitude IS NOT NULL "
    query += "AND depth IS NOT NULL "
    query += "AND latitude IS NOT NULL "
    query += "AND longitude IS NOT NULL "
    query += "ORDER BY ts"
    eq = db.df_read(query)
    eq.loc[:, 'latitude'] = eq.loc[:, 'latitude'].apply(lambda x: math.radians(x))
    eq.loc[:, 'longitude'] = eq.loc[:, 'longitude'].apply(lambda x: math.radians(x))
    eq.loc[:, 'key'] = 1
    return eq

def site_locs():
    query =  "SELECT * FROM loggers "
    query += "WHERE site_id IN"
    query += "  (SELECT site_id FROM sites "
    query += "  WHERE active = 1)"
    loggers = db.df_read(query)
    site = loggers.loc[loggers.model_id != 31, ['site_id', 'latitude',
                                                'longitude']]
    site = site.drop_duplicates('site_id')
    site.loc[:, 'latitude'] = site.loc[:, 'latitude'].apply(lambda x: math.radians(x))
    site.loc[:, 'longitude'] = site.loc[:, 'longitude'].apply(lambda x: math.radians(x))
    site.loc[:, 'key'] = 1
    return site

def distance():
    eq = eq_events()
    site = site_locs()
    eq_site = eq.merge(site, how='outer', on='key')
    eq_site.loc[:, 'dist'] = eq_site[['latitude_x', 'latitude_y', 'longitude_x', 'longitude_y']].apply(lambda row: 6371.01 * math.acos(math.sin(row.latitude_x)*math.sin(row.latitude_y) + math.cos(row.latitude_x)*math.cos(row.latitude_y)*math.cos(row.longitude_x - row.longitude_y)), axis=1)
    eq_site.loc[:, 'tot_dist'] = np.sqrt(eq_site.loc[:, 'dist']**2 + eq_site.loc[:, 'depth']**2)
    eq_site = eq_site.drop_duplicates(['ts', 'site_id', 'magnitude', 'tot_dist'])
    return eq_site[['site_id', 'eq_id', 'ts', 'magnitude', 'depth', 'dist', 'tot_dist']]
    
def surficial_displacement():
    query =  "SELECT * FROM marker_alerts "
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
    subsurface.loc[:, 'alert_level'] = subsurface.loc[:, 'con_mat'].map({'tp': 1, 'fp': 1, 'tn': 0, 'fn': 0})
    query = "SELECT site_id, logger_name FROM loggers"
    loggers = db.df_read(query)
    loggers_map = loggers.set_index('logger_name').to_dict()['site_id']
    subsurface.loc[:, 'site_id'] = subsurface.loc[:, 'tsm_name'].map(loggers_map)
    subsurface = subsurface.loc[subsurface.alert_level == 1, ['site_id', 'ts', 'alert_level']]
    return subsurface

def check_movt(eq_df, site_movt_df, days):
    index = eq_df.index.values[0]
    ts = eq_df['ts'].values[0]
    df = site_movt_df.loc[(site_movt_df.ts >= ts) & (site_movt_df.ts <= pd.to_datetime(ts)+timedelta(days)), :]
    if len(df) != 0:
        eq_df.loc[eq_df.index == index, 'movt'] = 1
    else:
        eq_df.loc[eq_df.index == index, 'movt'] = 0
    return eq_df
    

def check_eq_movt_corr(eq_df, movt_df, days):
    site_id = eq_df['site_id'].values[0]
    print('site #', site_id)
    site_movt_df = movt_df.loc[movt_df.site_id == site_id]
    per_eq_id = eq_df.groupby('eq_id', as_index=False)
    eq_df = per_eq_id.apply(check_movt, site_movt_df=site_movt_df,
                            days=days)
    return eq_df.reset_index(drop=True)

def confusion_matrix(matrix, eq_df):
    index = matrix.index[0]
    crit_dist = matrix['crit_dist'].values[0]
    eq_df.loc[eq_df.dist <= crit_dist, 'pred'] = 1
    eq_df.loc[eq_df.dist > crit_dist, 'pred'] = 0
    matrix.loc[matrix.index == index, 'tp'] = len(eq_df[(eq_df.movt == 1) & (eq_df.pred == 1)])
    matrix.loc[matrix.index == index, 'tn'] = len(eq_df[(eq_df.movt == 0) & (eq_df.pred == 0)])
    matrix.loc[matrix.index == index, 'fp'] = len(eq_df[(eq_df.movt == 0) & (eq_df.pred == 1)])
    matrix.loc[matrix.index == index, 'fn'] = len(eq_df[(eq_df.movt == 1) & (eq_df.pred == 0)])
    return matrix

def movt_per_mag(eq_df, ax):
    mag = eq_df['magnitude'].values[0]
    matrix = pd.DataFrame({'crit_dist': np.arange(0.5, 1000.5, 0.5)})
    matrix.loc[:, 'magnitude'] = mag
    dist_matrix = matrix.groupby('crit_dist', as_index=False)
    matrix = dist_matrix.apply(confusion_matrix, eq_df=eq_df).reset_index(drop=True)
    matrix.loc[:, 'tpr'] = matrix['tp'] / (matrix['tp'] + matrix['fn'])
    matrix.loc[:, 'fpr'] = matrix['fp'] / (matrix['fp'] + matrix['tn'])
    matrix = matrix.sort_values(['fpr', 'tpr'])
    matrix = matrix.fillna(0)
    ax.plot(matrix['fpr'].values, matrix['tpr'].values, '.-', label=mag)
    return matrix

def main(days, recompute=True, by_sklearn=True):
    if recompute:
        eq_dist = distance()
        eq_dist = eq_dist.loc[(eq_dist.ts >= '2017-05-01 07:00') & (eq_dist.ts <= '2019-02-26 08:00'), :]
        mag_count = Counter(eq_dist.magnitude)
        mag_list = list(k for k, v in mag_count.items() if v >= 500)
        eq_dist = eq_dist.loc[eq_dist.magnitude.isin(mag_list)]
        surficial = surficial_displacement()
        subsurface = subsurface_movt()
        movt_df = surficial.append(subsurface)
        movt_df = movt_df.loc[(movt_df.ts >= '2017-05-02 07:00') & (movt_df.ts <= '2019-03-01 08:00'), :]
        site_eq_dist = eq_dist.groupby('site_id', as_index=False)
        eq_movt = site_eq_dist.apply(check_eq_movt_corr, movt_df=movt_df,
                                     days=days).reset_index(drop=True)
        eq_movt.to_csv('eq_movt' + str(days) + '.csv', index=False)
    else:
        eq_movt = pd.read_csv('eq_movt' + str(days) + '.csv')
    
    all_matrix = pd.DataFrame()
    
    for i in range(1,7):
        if i != 6:
            part_eq_movt = eq_movt[(eq_movt.magnitude > i) & (eq_movt.magnitude <= i+1)]
            filename = 'ROC mag ' + str(i) + ' to ' + str(i+1)
        else:            
            part_eq_movt = eq_movt[(eq_movt.magnitude > i)]
            filename = 'ROC mag greater than ' + str(i)
        
        if len(part_eq_movt) != 0:
            fig = plt.figure(figsize=(10,10))
            ax = fig.add_subplot(111)
            num_colors = len(set(part_eq_movt.magnitude))
            ax = nonrepeat_colors(ax, num_colors, color='plasma')
            
            for mag in sorted(set(part_eq_movt.magnitude)):
                eq_mag_movt = part_eq_movt.loc[part_eq_movt.magnitude == mag, :]
                if by_sklearn:
                    y = eq_mag_movt.movt
                    scores = eq_mag_movt.dist
                    fpr, tpr, thresholds = metrics.roc_curve(y, scores)
                    yi = tpr - fpr
                    matrix = pd.DataFrame({'crit_dist': thresholds, 'tpr': tpr,
                                           'fpr': fpr, 'yi': yi})
                    optimal = matrix.loc[matrix.yi == max(yi), :]
                    label = 'mag ' + str(mag)
                    if len(optimal) != 0:
                        label += ': '+str(round(optimal['crit_dist'].values[0], 2)) + ' km'
                    ax.plot(fpr, tpr, '.-', label=label)
                    if len(optimal) != 0:
                        ax.plot(optimal['fpr'].values[0], optimal['tpr'].values[0], 'ko')
                    matrix.loc[:, 'magnitude'] = mag
                else:
                    matrix = movt_per_mag(eq_mag_movt, ax)
                all_matrix = all_matrix.append(matrix)
                
            ax.plot([0, 1], [0, 1], 'k--')
            ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
            ax.set_xlabel('FPR', fontsize = 14)
            ax.set_ylabel('TPR', fontsize = 14)
            plt.title(str(days) + 'D')
            plt.savefig(filename + ' D' + str(days) + '.png', facecolor='w', edgecolor='w', mode='w',
                        bbox_inches = 'tight')
            plt.close()
        
    return all_matrix


def optimal_threshold(matrix):
    yi = matrix['tpr'] - matrix['fpr']
    optimal = matrix.loc[matrix.yi == max(yi), :]
    mag_threshold = pd.DataFrame({'mag': [optimal['magnitude'].values[0]], 'crit_dist': [min(optimal.crit_dist)]})
    return mag_threshold


if __name__ == '__main__':
    start_time = datetime.now()
       
    matrix1 = main(1)#, recompute=False)
    grp_matrix1 = matrix1.groupby('magnitude', as_index=False)
    optimal1 = grp_matrix1.apply(optimal_threshold)
    optimal1 = optimal1.loc[optimal1.crit_dist < 1000, :]
    
    matrix2 = main(2)#, recompute=False)
    grp_matrix2 = matrix2.groupby('magnitude', as_index=False)
    optimal2 = grp_matrix2.apply(optimal_threshold)
    optimal2 = optimal2.loc[optimal2.crit_dist < 1000, :]

    matrix3 = main(3)#, recompute=False)
    grp_matrix3 = matrix3.groupby('magnitude', as_index=False)
    optimal3 = grp_matrix3.apply(optimal_threshold)
    optimal3 = optimal3.loc[optimal3.crit_dist < 1000, :]
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(optimal1.mag, optimal1.crit_dist, '.-', label='1D')
    ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
    ax.set_xlabel('magnitude', fontsize = 14)
    ax.set_ylabel('critical distance', fontsize = 14)
    plt.savefig('optimal_threshold' + ' D' + str(1) + '.png', facecolor='w', edgecolor='w', mode='w',
                bbox_inches = 'tight')

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(optimal2.mag, optimal2.crit_dist, '.-', label='2D')
    ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
    ax.set_xlabel('magnitude', fontsize = 14)
    ax.set_ylabel('critical distance', fontsize = 14)
    plt.savefig('optimal_threshold' + ' D' + str(2) + '.png', facecolor='w', edgecolor='w', mode='w',
                bbox_inches = 'tight')

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(optimal3.mag, optimal3.crit_dist, '.-', label='3D')
    ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
    ax.set_xlabel('magnitude', fontsize = 14)
    ax.set_ylabel('critical distance', fontsize = 14)
    plt.savefig('optimal_threshold' + ' D' + str(3) + '.png', facecolor='w', edgecolor='w', mode='w',
                bbox_inches = 'tight')
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    num_colors = 3
    ax = nonrepeat_colors(ax, num_colors, color='plasma')
    ax.plot(optimal1.mag, optimal1.crit_dist, '.-', label='1D')
    ax.plot(optimal2.mag, optimal2.crit_dist, '.-', label='2D')
    ax.plot(optimal3.mag, optimal3.crit_dist, '.-', label='3D')
    ax.legend(loc = 'lower right', fancybox = True, framealpha = 0.5)
    ax.set_xlabel('magnitude', fontsize = 14)
    ax.set_ylabel('critical distance', fontsize = 14)
    plt.savefig('optimal_threshold.png', facecolor='w', edgecolor='w', mode='w',
                bbox_inches = 'tight')

    print ('runtime =', datetime.now() - start_time)