import pandas as pd
from datetime import date, time, datetime, timedelta

import rtwindow as rtw
import querydb as qdb
import proc
import plotterlib as plotter

def proc_data(tsm_name, endTS, startTS, sc, hour_interval, fixpoint):
    tsm_props = qdb.get_tsm_list(tsm_name)[0]
    
    #end
    if endTS == '':
        window, config = rtw.get_window()
    else:
        end = pd.to_datetime(endTS)
        end_year=end.year
        end_month=end.month
        end_day=end.day
        end_hour=end.hour
        end_minute=end.minute
        if end_minute<30:end_minute=0
        else:end_minute=30
        end=datetime.combine(date(end_year,end_month,end_day),time(end_hour,end_minute,0))
        window, config = rtw.get_window(end)

    if startTS != '':
        #start
        start = pd.to_datetime(startTS)
        start_year=start.year
        start_month=start.month
        start_day=start.day
        start_hour=start.hour
        start_minute=start.minute
        if start_minute<30:start_minute=0
        else:start_minute=30
        window.start=datetime.combine(date(start_year,start_month,start_day),time(start_hour,start_minute,0))
        #offsetstart
        window.offsetstart = window.start - timedelta(days=(sc['subsurface']['num_roll_window_ops']*window.numpts-1)/48.)

    #colpos interval
    if hour_interval == '':
        if int((window.end-window.start).total_seconds() / (3600 * 24)) <= 5:
            hour_interval = 4
        else:
            hour_interval = 24
            
    sc['subsurface']['col_pos_interval'] = str(hour_interval) + 'H'
    sc['subsurface']['num_col_pos'] = int((window.end-window.start).total_seconds() / (3600 * hour_interval)) + 1
    
    if fixpoint in ['top', 'bottom']:
        sc['subsurface']['column_fic'] = fixpoint

    data = proc.proc_data(tsm_props, window, sc)

    num_nodes = tsm_props.nos
    seg_len = tsm_props.seglen
    
    tilt = data.tilt[window.start:window.end]
    tilt = tilt.reset_index().sort_values('ts',ascending=True)
    tilt = tilt[['ts', 'node_id', 'xz', 'xy', 'vel_xz', 'vel_xy']]

    return tilt, window, sc, tsm_props

def colpos(tilt, window, sc, tsm_props):
    # compute column position
    colposdf = plotter.compute_colpos(window, sc, tilt, tsm_props)
    colposdf = colposdf.rename(columns = {'cs_xz': 'downslope', 'cs_xy': 'latslope', 'x': 'depth'})
    colposdf['ts'] = colposdf['ts'].apply(lambda x: str(x))
    colposdf = colposdf[['ts', 'node_id', 'depth', 'latslope', 'downslope']]
    
    return colposdf

def velocity(tilt, window, sc, num_nodes):
    #velplots
    vel = tilt.loc[(tilt.ts >= window.start) & (tilt.ts <= window.end)]
    #vel_xz
    vel_xz = vel[['ts', 'vel_xz', 'node_id']]
    velplot_xz,L2_xz,L3_xz = plotter.vel_classify(vel_xz, sc, num_nodes, linearvel=False)
    #vel_xy
    vel_xy = vel[['ts', 'vel_xy', 'node_id']]
    velplot_xy,L2_xy,L3_xy = plotter.vel_classify(vel_xy, sc, num_nodes, linearvel=False)
    
    L2_xz['ts'] = L2_xz['ts'].apply(lambda x: str(x))
    L3_xz['ts'] = L3_xz['ts'].apply(lambda x: str(x))
    L2_xy['ts'] = L2_xy['ts'].apply(lambda x: str(x))
    L3_xy['ts'] = L3_xy['ts'].apply(lambda x: str(x))

    vel_xz = pd.DataFrame({'L2': [L2_xz], 'L3': [L3_xz]})
    vel_xy = pd.DataFrame({'L2': [L2_xy], 'L3': [L3_xy]})
    veldf = pd.DataFrame({'downslope': [vel_xz], 'latslope': [vel_xy]})

    return veldf

def displacement(tilt, window, sc, num_nodes):
    # displacement plot offset
    xzd_plotoffset = plotter.plotoffset(tilt, disp_offset = 'mean')

    #zeroing and offseting xz,xy
    df0off = plotter.disp0off(tilt, window, sc, xzd_plotoffset, num_nodes)
    df0off = df0off.rename(columns = {'xz': 'downslope', 'xy': 'latslope'})
    df0off = df0off.reset_index()
    df0off['ts'] = df0off['ts'].apply(lambda x: str(x))
    df0off = df0off[['ts', 'node_id', 'downslope', 'latslope']]
    
    inc_df = plotter.node_annotation(tilt, num_nodes)
    inc_df = inc_df.rename(columns = {'text_xz': 'downslope_annotation', 'text_xy': 'latslope_annotation'})
    inc_df = inc_df[['node_id', 'downslope_annotation', 'latslope_annotation']]
    
    cs_df = plotter.cum_surf(tilt, xzd_plotoffset, num_nodes)
    cs_df = cs_df.rename(columns = {'xz': 'downslope', 'xy': 'latslope'})
    cs_df = cs_df.reset_index()
    cs_df['ts'] = cs_df['ts'].apply(lambda x: str(x))
    cs_df = cs_df[['ts', 'downslope', 'latslope']]
        
    dispdf = pd.DataFrame({'disp': [df0off], 'annotation': [inc_df], 'cumulative': [cs_df], 'cml_base': [xzd_plotoffset * num_nodes]})

    return dispdf

def vcdgen(tsm_name, endTS='', startTS='', hour_interval='', fixpoint='bottom'):
    
    sc = qdb.memcached()
    
    tilt, window, sc, tsm_props = proc_data(tsm_name, endTS, startTS, sc, hour_interval, fixpoint)    

    dispdf = displacement(tilt, window, sc, tsm_props.nos)
    
    colposdf = colpos(tilt, window, sc, tsm_props)
    veldf = velocity(tilt, window, sc, tsm_props.nos)

    vcd = pd.DataFrame({'v': [veldf], 'c': [colposdf], 'd': [dispdf]})
    vcd_json = vcd.to_json(orient="records", date_format="iso")

    return vcd_json
    
################################################################################
    
if __name__ == '__main__':
    
    json = vcdgen('magta', endTS='2017-06-09 19:30')

    v_L2 = pd.DataFrame(pd.read_json(json)['v'].values[0][0]['L2']).sort_values(['node_id', 'ts'])
    v_L3 = pd.DataFrame(pd.read_json(json)['v'].values[0][0]['L3']).sort_values(['node_id', 'ts'])
    
    c = pd.DataFrame(pd.read_json(json)['c'].values[0])
    
    d_disp = pd.DataFrame(pd.DataFrame(pd.read_json(json)['d'].values[0])['disp'].values[0]).sort_values(['node_id', 'ts'])
    d_annotation = pd.DataFrame(pd.DataFrame(pd.read_json(json)['d'].values[0])['annotation'].values[0])
    d_cumulative = pd.DataFrame(pd.DataFrame(pd.read_json(json)['d'].values[0])['cumulative'].values[0])
    d_cml_base = pd.DataFrame(pd.read_json(json)['d'].values[0])['cml_base'].values[0]