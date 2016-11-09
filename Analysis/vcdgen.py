import pandas as pd
from datetime import date, time, datetime, timedelta

import rtwindow as rtw
import querySenslopeDb as q
import genproc as g
import RealtimePlotter as plotter

def proc(func, colname, endTS, startTS, day_interval, fixpoint):
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
        config.io.col_pos_interval = str(day_interval) + 'D'
        config.io.num_col_pos = int((window.end - window.start).days/day_interval + 1)  
    
    monitoring = g.genproc(col[0], window, config, fixpoint)

    num_nodes = monitoring.colprops.nos
    seg_len = monitoring.colprops.seglen
    monitoring_vel = monitoring.vel.reset_index()[['ts', 'id', 'xz', 'xy', 'vel_xz', 'vel_xy']]
    monitoring_vel = monitoring_vel.loc[(monitoring_vel.ts >= window.start)&(monitoring_vel.ts <= window.end)]

    return monitoring_vel, window, config, num_nodes, seg_len

def colpos_json(monitoring_vel, window, config, num_nodes, seg_len, fixpoint):
    # compute column position
    colposdf = plotter.compute_colpos(window, config, monitoring_vel, num_nodes, seg_len, fixpoint=fixpoint)
    colposdfj = colposdf.rename(columns = {'cs_xz': 'downslope', 'cs_xy': 'latslope', 'x': 'depth'})
#    colposdfj = colposdfj[0:3]
    colposdf_json = colposdfj[['ts', 'id', 'downslope', 'latslope', 'depth']].to_json(orient="records", date_format="iso")
    return colposdf, colposdf_json

def colpos(colname, endTS='', startTS='', day_interval=1, fixpoint='bottom'):

    monitoring_vel, window, config, num_nodes, seg_len = proc('colpos', colname, endTS, startTS, day_interval, fixpoint)
    colposdf, colposdf_json = colpos_json(monitoring_vel, window, config, num_nodes, seg_len, fixpoint)

#    #############################
#    show_part_legend = False
#    plotter.plot_column_positions(colposdf,colname,window.end, show_part_legend, config)
#    #############################

    return colposdf_json

def velocity_json(monitoring_vel, window, config, num_nodes):
    #velplots
    vel = monitoring_vel.loc[(monitoring_vel.ts >= window.start) & (monitoring_vel.ts <= window.end)]
    #vel_xz
    vel_xz = vel[['ts', 'vel_xz', 'id']]
    velplot_xz,L2_xz,L3_xz = plotter.vel_classify(vel_xz, config, num_nodes, linearvel=False)
    #vel_xy
    vel_xy = vel[['ts', 'vel_xy', 'id']]
    velplot_xy,L2_xy,L3_xy = plotter.vel_classify(vel_xy, config, num_nodes, linearvel=False)

    velplot = velplot_xz, velplot_xy, L2_xz, L2_xy, L3_xz, L3_xy
    
    L2 = L2_xz.append(L2_xy)
    L3 = L3_xz.append(L3_xy)    
#    L2 = L2[0:3]
#    L3 = L3[0:3]
    
    L2_json = L2.to_json(orient="records", date_format="iso")
    L3_json = L3.to_json(orient="records", date_format="iso")
    velocity = dict({'L2': L2_json, 'L3': L3_json})
    velocity = '[' + str(velocity).replace('\'', '').replace('L2', '"L2"').replace('L3', '"L3"') + ']'

    return velplot, velocity

def velocity(colname, endTS='', startTS=''):

    monitoring_vel, window, config, num_nodes, seg_len = proc('velocity', colname, endTS, startTS, '', '')    
    velplot, velocity = velocity_json(monitoring_vel, window, config, num_nodes)

#    #############################
#    plotvel = True
#    empty = pd.DataFrame({'ts':[], 'id':[]})
#    xzd_plotoffset = 0
#    plotter.plot_disp_vel(empty, empty, empty, colname, window, config, plotvel, xzd_plotoffset, num_nodes, velplot, False)
#    #############################

    return velocity

def displacement_json(monitoring_vel, window, config, num_nodes, fixpoint):
    # displacement plot offset
    xzd_plotoffset = 0

    #zeroing and offseting xz,xy
    df0off = plotter.disp0off(monitoring_vel, window, config, xzd_plotoffset, num_nodes, fixpoint=fixpoint)
    df0offj = df0off.rename(columns = {'xz': 'downslope', 'xy': 'latslope'})
#    df0offj = df0offj[0:3]
    df0off_json = df0offj.reset_index()[['ts', 'id', 'downslope', 'latslope']].to_json(orient="records", date_format="iso")

    return df0off, df0off_json

def displacement(colname, endTS='', startTS='', fixpoint='bottom'):
    
    monitoring_vel, window, config, num_nodes, seg_len = proc('velocity', colname, endTS, startTS, '', fixpoint)    
    df0off, df0off_json = displacement_json(monitoring_vel, window, config, num_nodes, fixpoint)

#    #############################
#    velplot = ''
#    xzd_plotoffset = 0
#    plotvel = False
#    empty = pd.DataFrame({'ts':[], 'id':[]})
#    plotter.plot_disp_vel(empty, df0off, empty, colname, window, config, plotvel, xzd_plotoffset, num_nodes, velplot, False)
#    #############################

    return df0off_json

def vcdgen(colname, endTS='', startTS='', day_interval=1, fixpoint='bottom'):
    
    monitoring_vel, window, config, num_nodes, seg_len = proc('velocity', colname, endTS, startTS, day_interval, fixpoint)    

    colposdf, colposdf_json = colpos_json(monitoring_vel, window, config, num_nodes, seg_len, fixpoint)

#    #############################
#    show_part_legend = False
#    plotter.plot_column_positions(colposdf,colname,window.end, show_part_legend, config)
#    #############################
    
    df0off, df0off_json = displacement_json(monitoring_vel, window, config, num_nodes, fixpoint)
    velplot, velocity = velocity_json(monitoring_vel, window, config, num_nodes)

#    #############################
#    plotvel = True
#    xzd_plotoffset = 0
#    empty = pd.DataFrame({'ts':[], 'id':[]})
#    plotter.plot_disp_vel(empty, df0off, empty, colname, window, config, plotvel, xzd_plotoffset, num_nodes, velplot, False)
#    #############################

    vcd = dict({'v': velocity, 'c': colposdf_json, 'd': df0off_json})
    vcd = '[' + str(vcd).replace('\'', '').replace('v:', '"v":').replace('c:', '"c":').replace('d:', '"d":') + ']'

    return vcd
    
######################################
#    
#if __name__ == '__main__':
#    start = datetime.now()
#    c=colpos('magta', endTS='2016-10-12 12:00')
#    print "runtime =", str(datetime.now() - start)