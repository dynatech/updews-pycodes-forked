from datetime import datetime, timedelta
import os
import pandas as pd
from sqlalchemy import create_engine
import sys

import rtwindow as rtw
import proc as p
#import AlertAnalysis as A
#import ColumnPlotter as plotter
import alertlib as lib

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

def create_tsm_alerts():
    
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `tsm_alerts` ("
    query += "  `ta_id` INT(3) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL,"
    query += "  `tsm_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `alert_level` CHAR(2) NOT NULL,"
    query += "  `ts_updated` TIMESTAMP NOT NULL,"
    query += "  PRIMARY KEY (`ta_id`),"
    query += "  UNIQUE INDEX `uq_tsm_alerts` (`ts` ASC, `tsm_id` ASC),"
    query += "  INDEX `fk_node_alerts_tsm_sensors1_idx` (`tsm_id` ASC),"
    query += "  CONSTRAINT `fk_node_alerts_tsm_sensors1`"
    query += "    FOREIGN KEY (`tsm_id`)"
    query += "    REFERENCES `tsm_sensors` (`tsm_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()


def alert_toDB(df, table_name, window):
    
    query = "SELECT * FROM senslopedb.%s WHERE site = '%s' and timestamp <= '%s' AND updateTS >= '%s' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0], window.end, window.end-timedelta(hours=1))
    
    try:
        df2 = q.GetDBDataFrame(query)
    except:
        df2 = pd.DataFrame()
    
    if len(df2) == 0 or df2.alert.values[0] != df.alert.values[0]:
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        
    elif df2.alert.values[0] == df.alert.values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.%s SET updateTS='%s' WHERE site = '%s' and source = 'sensor' and alert = '%s' and timestamp = '%s'" %(table_name, window.end, df2.site.values[0], df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
        cur.execute(query)
        db.commit()
        db.close()

def write_site_alert(site, window):
    if site != 'messb' and site != 'mesta':
        site = site[0:3] + '%'
        query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site LIKE '%s' and timestamp <= '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(site, window.end, window.end-timedelta(hours=0.5))
    else:
        query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site = '%s' and timestamp <= '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(site, window.end, window.end-timedelta(hours=0.5))
        
    df = q.GetDBDataFrame(query)

    if 'L3' in list(df.alert.values):
        site_alert = 'L3'
    elif 'L2' in list(df.alert.values):
        site_alert = 'L2'
    elif 'L0' in list(df.alert.values):
        site_alert = 'L0'
    else:
        site_alert = 'ND'
        
    if site == 'messb':
        site = 'msl'
    if site == 'mesta':
        site = 'msu'
        
    output = pd.DataFrame({'timestamp': [window.end], 'site': [site[0:3]], 'source': ['sensor'], 'alert': [site_alert], 'updateTS': [window.end]})
    
    alert_toDB(output, 'site_level_alert', window)
    
    return output


def main(tsm_name='', end=datetime.now(), end_mon=False):
    if tsm_name == '':
        tsm_name = sys.argv[1].lower()
    
    window,config = rtw.getwindow(end)

    tsm_props = q.GetTSMList(tsm_name)[0]
    proc = p.proc(tsm_props, window, config, config.io.column_fix)
        
    tilt = proc.tilt[window.start:window.end]
    lgd = proc.lgd
    tilt = tilt.reset_index().sort_values('ts',ascending=True)
    nodal_tilt = tilt.groupby('id')     
        
    alert = nodal_tilt.apply(lib.node_alert2, colname=tsm_props.tsm_name, num_nodes=tsm_props.nos, T_disp=config.io.t_disp, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3, k_ac_ax=config.io.k_ac_ax, lastgooddata=lgd,window=window,config=config)
    alert = lib.column_alert(alert, config.io.num_nodes_to_check, config.io.k_ac_ax)

    if 'L3' in list(alert.col_alert.values):
        site_alert = 'L3'
    elif 'L2' in list(alert.col_alert.values):
        site_alert = 'L2'
    else:
        site_alert = min(lib.getmode(list(alert.col_alert.values)))
        
    column_level_alert = pd.DataFrame({'ts': [window.end], 'tsm_id': [tsm_props.tsm_id], 'alert_level': [site_alert], 'ts_updated': [window.end]})
    
#    if site_alert in ('L2', 'L3'):
#        column_level_alert = A.main(tsm_props.tsm_name, window.end)
#
#    alert_toDB(column_level_alert, 'column_level_alert', window)
    
    print column_level_alert
    
#    write_site_alert(tsm_props.tsm_name, window)

########################
#
#    if tsm_props.tsm_name == 'mesta':
#        colname = 'msu'
#    elif tsm_props.tsm_name == 'messb':
#        colname = 'msl'
#    else:
#        colname = tsm_props.tsm_name[0:3]
#    query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' and source = 'public' and timestamp <= '%s' and updateTS >= '%s' ORDER BY updateTS DESC LIMIT 1" %(colname, window.end, window.end-timedelta(hours=0.5))
#    public_alert = q.GetDBDataFrame(query)
#    if public_alert.alert.values[0] != 'A0':
#        plot_time = ['07:30:00', '19:30:00']
#        if str(window.end.time()) in plot_time or end_mon:
#            plotter.main(proc, window, config, plotvel_start=window.end-timedelta(hours=3), plotvel_end=window.end, realtime=False)
#    elif RoundTime(pd.to_datetime(public_alert.timestamp.values[0])) == RoundTime(window.end):
#        plotter.main(proc, window, config, plotvel_start=window.end-timedelta(hours=3), plotvel_end=window.end, realtime=False)
#
########################
    
    return column_level_alert

################################################################################

if __name__ == "__main__":
    start = datetime.now()
    main()
    print 'run time =', datetime.now()-start