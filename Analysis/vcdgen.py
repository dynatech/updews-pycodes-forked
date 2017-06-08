import pandas as pd
from datetime import date, time, datetime, timedelta

import rtwindow as rtw
import querySenslopeDb as q
import genproc as g
import ColumnPlotter as plotter

def proc(func, colname, endTS, startTS, hour_interval, fixpoint):
    col = q.GetSensorList(colname)
    
    #end
    if endTS == '':
        window, config = rtw.getwindow()
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
        window, config = rtw.getwindow(end)

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
        window.offsetstart = window.start - timedelta(days=(config.io.num_roll_window_ops*window.numpts-1)/48.)

    if func == 'colpos' or func == 'vcdgen':
        #colpos interval
        if hour_interval == '':
            if int((window.end-window.start).total_seconds() / (3600 * 24)) <= 5:
                hour_interval = 4
            else:
                hour_interval = 24
        config.io.col_pos_interval = str(hour_interval) + 'H'
        config.io.num_col_pos = int((window.end-window.start).total_seconds() / (3600 * hour_interval)) + 1
        
    if func == 'displacement' or func == 'colpos':
        comp_vel = False
    else:
        comp_vel = True
    
    monitoring = g.genproc(col[0], window, config, fixpoint, comp_vel=comp_vel)

    num_nodes = monitoring.colprops.nos
    seg_len = monitoring.colprops.seglen
    if comp_vel == True:
        monitoring_vel = monitoring.vel.reset_index()[['ts', 'id', 'xz', 'xy', 'vel_xz', 'vel_xy']]
    else:
        monitoring_vel = monitoring.vel.reset_index()[['ts', 'id', 'xz', 'xy']]
    monitoring_vel = monitoring_vel.loc[(monitoring_vel.ts >= window.start)&(monitoring_vel.ts <= window.end)]

    return monitoring_vel, window, config, num_nodes, seg_len

def colpos(monitoring_vel, window, config, num_nodes, seg_len, fixpoint):
    # compute column position
    colposdf = plotter.compute_colpos(window, config, monitoring_vel, num_nodes, seg_len, fixpoint=fixpoint)
    colposdf = colposdf.rename(columns = {'cs_xz': 'downslope', 'cs_xy': 'latslope', 'x': 'depth'})
    colposdf['ts'] = colposdf['ts'].apply(lambda x: str(x))
    colposdf = colposdf[['ts', 'id', 'depth', 'latslope', 'downslope']]
    
    return colposdf

def velocity(monitoring_vel, window, config, num_nodes):
    #velplots
    vel = monitoring_vel.loc[(monitoring_vel.ts >= window.start) & (monitoring_vel.ts <= window.end)]
    #vel_xz
    vel_xz = vel[['ts', 'vel_xz', 'id']]
    velplot_xz,L2_xz,L3_xz = plotter.vel_classify(vel_xz, config, num_nodes, linearvel=False)
    #vel_xy
    vel_xy = vel[['ts', 'vel_xy', 'id']]
    velplot_xy,L2_xy,L3_xy = plotter.vel_classify(vel_xy, config, num_nodes, linearvel=False)

    L2 = L2_xz.append(L2_xy)
    L3 = L3_xz.append(L3_xy) 
    
    L2['ts'] = L2['ts'].apply(lambda x: str(x))
    L3['ts'] = L3['ts'].apply(lambda x: str(x))
    
    veldf = pd.DataFrame({'L2': [L2], 'L3': [L3]})

    return veldf

def displacement(monitoring_vel, window, config, num_nodes, fixpoint):
    # displacement plot offset
    xzd_plotoffset = plotter.plotoffset(monitoring_vel, disp_offset = 'mean')

    #zeroing and offseting xz,xy
    df0off = plotter.disp0off(monitoring_vel, window, config, xzd_plotoffset, num_nodes, fixpoint=fixpoint)
    df0off = df0off.rename(columns = {'xz': 'downslope', 'xy': 'latslope'})
    df0off = df0off.reset_index()
    df0off['ts'] = df0off['ts'].apply(lambda x: str(x))
    df0off = df0off[['ts', 'downslope', 'latslope']]
    
    inc_df = plotter.node_annotation(monitoring_vel, num_nodes)
    inc_df = inc_df.rename(columns = {'text_xz': 'downslope_annotation', 'text_xy': 'latslope_annotation'})
    inc_df = inc_df[['id', 'downslope_annotation', 'latslope_annotation']]
        
    dispdf = pd.DataFrame({'d': [df0off], 'annotation': [inc_df]})

    return dispdf

def vcdgen(colname, endTS='', startTS='', hour_interval='', fixpoint='bottom'):
    
    monitoring_vel, window, config, num_nodes, seg_len = proc('vcdgen', colname, endTS, startTS, hour_interval, fixpoint)    

    dispdf = displacement(monitoring_vel, window, config, num_nodes, fixpoint)
    
    colposdf = colpos(monitoring_vel, window, config, num_nodes, seg_len, fixpoint)
    veldf = velocity(monitoring_vel, window, config, num_nodes)

    vcd = pd.DataFrame({'v': [veldf], 'c': [colposdf], 'd': [dispdf]})
    vcd_json = vcd.to_json(orient="records", date_format="iso")

    return vcd_json