from datetime import datetime
import os
import numpy as np
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import volatile.memory as mem


def all_site_coord():
    """Retrieves coordinates of sites from memcache

    Returns:
        dataframe: Record of coordinates of sites.
    
    """
    
    df = mem.get('df_dyna_rain_gauges')[['site_id', 'latitude', 'longitude']]
    df = df.dropna()
    df = df.drop_duplicates('site_id')
    df = df.sort_values('site_id')
    return df

def to_mysql(df):
    """Writes in rainfall_priorities the distance of 4 active nearby
    rain gauges from the site.
    
    Args:
        df (dataframe): Record of 4 nearby rain gauges with 
        its distance from the site.

    """
    written_df = mem.get('df_rain_priorities')
    combined = written_df.append(df, ignore_index=True, sort=False)
    combined = combined.append(written_df, ignore_index=True, sort=False)
    combined = combined.drop_duplicates(['site_id', 'rain_id'], keep=False)

    if len(combined) > 0:
        qdb.write_rain_priorities(combined)

    
def get_distance(site_coord, rg_coord):
    """Computes for distance of nearby rain gauges from the site.
    
    Args:
        site_coord (dataframe): Record of coordinates of sites.
        rg_coord (str): Record of coordinates of rain gauges.

    Returns:
        dataframe: Nearest 4 rain gauges with its distance from the site.
    
    """

    site_id = site_coord['site_id'].values[0]
    site_lat = site_coord['latitude'].values[0]
    site_lon = site_coord['longitude'].values[0]

    rg_coord['latitude'] = rg_coord['latitude'].apply(lambda x: float(x))
    rg_coord['longitude'] = rg_coord['longitude'].apply(lambda x: float(x))

    rg_coord['dlat'] = rg_coord['latitude'].apply(lambda x: x - site_lat)
    rg_coord['dlon'] = rg_coord['longitude'].apply(lambda x: x - site_lon)
    rg_coord['dlat'] = np.radians(rg_coord.dlat)
    rg_coord['dlon'] = np.radians(rg_coord.dlon)

    rg_coord['a1'] = rg_coord['dlat'].apply(lambda x: np.sin(x/2)**2)
    rg_coord['a3'] = rg_coord['latitude'].apply(lambda x: np.cos(np.radians(x)))
    rg_coord['a4'] = rg_coord['dlon'].apply(lambda x: np.sin(x/2)**2)
    
    rg_coord['a'] = rg_coord['a1'] + (np.cos(np.radians(site_lat)) * \
                                      rg_coord['a3'] * rg_coord['a4'])
    rg_coord['c']= 2 * np.arctan2(np.sqrt(rg_coord.a),np.sqrt(1-rg_coord.a))
    rg_coord['distance']= 6371 * rg_coord.c
    rg_coord = rg_coord.sort_values('distance', ascending = True)
    
    nearest_rg = rg_coord[0:4]
    nearest_rg['site_id'] = site_id
    nearest_rg = nearest_rg[['site_id', 'rain_id', 'distance']]
    
    return nearest_rg

def main(site_code=''):
    """Writes in rainfall_priorities information on nearest rain gauges
    from the project sites for rainfall alert analysis

    """

    start = datetime.now()
    qdb.print_out(start)
    
    coord = all_site_coord()
    if site_code == '':
        try:
            site_code = sys.argv[1].lower()
            site_code = site_code.replace(' ', '').split(',')
        except:
            pass
    else:
        site_code = site_code.replace(' ', '').split(',')
    if site_code != '':
        sites = mem.get('df_sites')
        site_id = sites.loc[sites.site_code.isin(site_code), 'site_id']
        coord = coord.loc[coord.site_id.isin(site_id), :]

    rg_coord = mem.get('df_rain_gauges')
    rg_coord = rg_coord[rg_coord.date_deactivated.isnull()]
    site_coord = coord.groupby('site_id', as_index=False)
    nearest_rg = site_coord.apply(get_distance, rg_coord=rg_coord)
    nearest_rg['distance'] = np.round(nearest_rg.distance,2)
    nearest_rg = nearest_rg.reset_index(drop=True)
    
    if qdb.does_table_exist('rainfall_priorities') == False:
        #Create a NOAH table if it doesn't exist yet
        qdb.create_rainfall_priorities()

    to_mysql(nearest_rg)
    
    qdb.print_out('runtime = %s' %(datetime.now() - start))
    
if __name__ == "__main__":
    main()
