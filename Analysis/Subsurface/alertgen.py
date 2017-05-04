from datetime import datetime, timedelta
import os
import pandas as pd
import sys

import alertlib as lib
import proc as p
import rtwindow as rtw
import trendingalert as t
#import ColumnPlotter as plotter

#include the path of outer folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

def create_tsm_alerts():
    
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `tsm_alerts` ("
    query += "  `ta_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL DEFAULT '2010-01-01 00:00:00',"
    query += "  `tsm_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `alert_level` TINYINT(2) NOT NULL,"
    query += "  `ts_updated` TIMESTAMP NOT NULL DEFAULT '2010-01-01 00:00:00',"
    query += "  PRIMARY KEY (`ta_id`),"
    query += "  UNIQUE INDEX `uq_tsm_alerts` (`ts` ASC, `tsm_id` ASC),"
    query += "  INDEX `fk_tsm_alerts_tsm_sensors1_idx` (`tsm_id` ASC),"
    query += "  CONSTRAINT `fk_tsm_alerts_tsm_sensors1`"
    query += "    FOREIGN KEY (`tsm_id`)"
    query += "    REFERENCES `tsm_sensors` (`tsm_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()

def create_site_alerts():
    
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `site_alerts` ("
    query += "  `sa_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL DEFAULT '2010-01-01 00:00:00',"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `alert_source` CHAR(10) NOT NULL,"
    query += "  `alert_level` CHAR(12) NOT NULL,"
    query += "  `ts_updated` TIMESTAMP DEFAULT '2010-01-01 00:00:00',"
    query += "  PRIMARY KEY (`sa_id`),"
    query += "  UNIQUE INDEX `uq_site_alerts` (`ts` ASC, `site_id` ASC, `alert_source` ASC),"
    query += "  INDEX `fk_tsm_alerts_sites1_idx` (`site_id` ASC),"
    query += "  CONSTRAINT `fk_tsm_alerts_sites1`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()

def alert_toDB(df, table_name):

    if q.DoesTableExist(table_name) == False:
        #Create a tsm_alerts table if it doesn't exist yet
        if table_name == 'tsm_alerts':
            create_tsm_alerts()
        #Create a site_alerts table if it doesn't exist yet
        elif table_name == 'site_alerts':
            create_site_alerts
        else:
            print 'unrecognized table:', table_name
            return
 
    query = "SELECT * FROM %s WHERE" %table_name

    if table_name == 'tsm_alerts':
        query += " tsm_id = '%s'" %df['tsm_id'].values[0]
    else:
        query += " site_id = '%s' and alert_source = '%s'" %(df['site_id'].values[0], df['alert_source'].values[0])

    query += " and ts <= '%s' ORDER BY ts DESC LIMIT 1" %df['ts_updated'].values[0]

    df2 = q.GetDBDataFrame(query)

    if len(df2) == 0 or df2['alert_level'].values[0] != df['alert_level'].values[0]:
        q.PushDBDataFrame(df, table_name, index=False)
        
    elif df2['alert_level'].values[0] == df['alert_level'].values[0] and df2['ts_updated'].values[0] < df['ts_updated'].values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE %s SET ts_updated = '%s' WHERE" %(table_name, df['ts_updated'].values[0])
        if table_name == 'tsm_alerts':
            query += " ta_id = %s" %df2['ta_id'].values[0]
        else:
            query += " sa_id = %s" %df2['sa_id'].values[0]
        cur.execute(query)
        db.commit()
        db.close()

def write_site_alerts(site, window):
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
    
    alert_toDB(output, 'site_level_alert')
    
    return output


def main(tsm_name='', end='', end_mon=False):
    if tsm_name == '':
        tsm_name = sys.argv[1].lower()

    if end == '':
        try:
            end = datetime.now()
        except:
            end = pd.to_datetime(sys.argv[2])
    
    window,config = rtw.getwindow(end)

    tsm_props = q.GetTSMList(tsm_name)[0]
    proc = p.proc(tsm_props, window, config, config.io.column_fix)
        
    tilt = proc.tilt[window.start:window.end]
    lgd = proc.lgd
    tilt = tilt.reset_index().sort_values('ts',ascending=True)
    nodal_tilt = tilt.groupby('id', as_index=False)     
        
    alert = nodal_tilt.apply(lib.node_alert, colname=tsm_props.tsm_name, num_nodes=tsm_props.nos, T_disp=config.io.t_disp, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3, k_ac_ax=config.io.k_ac_ax, lastgooddata=lgd,window=window,config=config).reset_index(drop=True)
    alert = lib.column_alert(alert, config.io.num_nodes_to_check)

    valid_nodes_alert = alert.loc[~alert.id.isin(proc.inv)]

    if max(valid_nodes_alert['col_alert'].values) > 0:
        pos_alert = alert[alert.col_alert > 0]
        site_alert = t.main(pos_alert, tsm_props.tsm_id, window.end, proc.inv)
    else:
        site_alert = max(lib.getmode(list(valid_nodes_alert['col_alert'].values)))
        
    tsm_alert = pd.DataFrame({'ts': [window.end], 'tsm_id': [tsm_props.tsm_id], 'alert_level': [site_alert], 'ts_updated': [window.end]})
    
    alert_toDB(tsm_alert, 'tsm_alerts')
    
    print tsm_alert
    
#    write_site_alerts(tsm_props.tsm_name, window)

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
    
    return tsm_alert, alert

################################################################################

if __name__ == "__main__":
    run_start = datetime.now()
#    for i in pd.date_range(start='2017-05-02 07:00', end='2017-05-02 13:00', freq='30min'):
#        main('magta', end=i)
    main()
    print 'run time =', datetime.now()-run_start