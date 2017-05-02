from datetime import timedelta
import os
import pandas as pd
from sqlalchemy import create_engine
import sys

import alertlib as lib

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

def create_tsm_alerts():
    
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `node_alerts` ("
    query += "  `na_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL,"
    query += "  `accel_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `disp_alert` TINYINT(2) NOT NULL DEFAULT -1,"
    query += "  `vel_alert` TINYINT(2) NOT NULL DEFAULT -1,"
    query += "  PRIMARY KEY (`Na_id`),"
    query += "  UNIQUE INDEX `uq_node_alerts` (`ts` ASC, `accel_id` ASC),"
    query += "  INDEX `fk_node_alerts_accelerometers1_idx` (`accel_id` ASC),"
    query += "  CONSTRAINT `fk_node_alerts_accelerometers1`"
    query += "    FOREIGN KEY (`accel_id`)"
    query += "    REFERENCES `tsm_sensors` (`accel_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()

def trending_alertgen(trending_alert, monitoring, lgd, window, config):
    endTS = pd.to_datetime(trending_alert['timestamp'].values[0])
    monitoring_vel = monitoring.vel[endTS-timedelta(3):endTS]
    monitoring_vel = monitoring_vel.reset_index().sort_values('ts',ascending=True)
    nodal_dv = monitoring_vel.groupby('id')     
    
    alert = nodal_dv.apply(lib.node_alert, colname=monitoring.colprops.name, num_nodes=monitoring.colprops.nos, T_disp=config.io.t_disp, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3, k_ac_ax=config.io.k_ac_ax, lastgooddata=lgd,window=window,config=config)
    alert = lib.column_alert(alert, config.io.num_nodes_to_check, config.io.k_ac_ax)
    alert['timestamp']=endTS
    
    palert = alert.loc[(alert.col_alert == 'L2') | (alert.col_alert == 'L3')]

    if len(palert) != 0:
        palert['site']=monitoring.colprops.name
        palert = palert[['timestamp', 'site', 'disp_alert', 'vel_alert', 'col_alert']].reset_index()
        palert = palert[['timestamp', 'site', 'id', 'disp_alert', 'vel_alert', 'col_alert']]
        
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        for i in palert.index:
            try:
                palert.loc[palert.index == i].to_sql(name = 'node_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
            except:
                print 'data already written in senslopedb.node_level_alert'

    alert['TNL'] = alert['col_alert'].values
    
    if len(palert) != 0:
        for i in palert['id'].values:
            query = "SELECT * FROM senslopedb.node_level_alert WHERE site = '%s' and timestamp >= '%s' and id = %s" %(monitoring.colprops.name, endTS-timedelta(hours=3), i)
            nodal_palertDF = q.GetDBDataFrame(query)
            if len(nodal_palertDF) >= 3:
                palert_index = alert.loc[alert.id == i].index[0]
                alert.loc[palert_index]['TNL'] = max(lib.getmode(list(nodal_palertDF['col_alert'].values)))
            else:
                palert_index = alert.loc[alert.id == i].index[0]
                alert.loc[palert_index]['TNL'] = 'L0'
    
    not_working = q.GetNodeStatus(1).loc[q.GetNodeStatus(1).site == monitoring.colprops.name]['node'].values
    
    for i in not_working:
        alert = alert.loc[alert.id != i]
    
    if 'L3' in alert['TNL'].values:
        site_alert = 'L3'
    elif 'L2' in alert['TNL'].values:
        site_alert = 'L2'
    else:
        site_alert = min(lib.getmode(list(alert['TNL'].values)))
    
    alert_index = trending_alert.loc[trending_alert.timestamp == endTS].index[0]
    trending_alert.loc[alert_index] = [endTS, monitoring.colprops.name, 'sensor', site_alert]
    
    return trending_alert

def main(pos_alert, tsm_id, end):
        
    nodal_pos_alert = pos_alert.groupby('id')
    trending_alert = nodal_pos_alert.apply(trending_alertgen)
    
    if max(trending_alert['TNL'].values) > 1:
        site_alert = max(trending_alert['TNL'].values)
        
    

    
    return site_alert