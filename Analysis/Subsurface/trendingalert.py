from datetime import timedelta
import numpy as np
import os
import sys

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

def create_node_alerts():
    
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
    query = "CREATE TABLE `node_alerts` ("
    query += "  `na_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL,"
    query += "  `accel_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `disp_alert` TINYINT(1) NOT NULL DEFAULT 0,"
    query += "  `vel_alert` TINYINT(1) NOT NULL DEFAULT 0,"
    query += "  PRIMARY KEY (`Na_id`),"
    query += "  UNIQUE INDEX `uq_node_alerts` (`ts` ASC, `accel_id` ASC),"
    query += "  INDEX `fk_node_alerts_accelerometers1_idx` (`accel_id` ASC),"
    query += "  CONSTRAINT `fk_node_alerts_accelerometers1`"
    query += "    FOREIGN KEY (`accel_id`)"
    query += "    REFERENCES `accelerometers` (`accel_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    cur.execute(query)
    db.commit()
    db.close()

def trending_alertgen(pos_alert, tsm_id, end):
    
    query = "SELECT * FROM accelerometers WHERE tsm_id = %s and node_id = %s" %(tsm_id,pos_alert['id'].values[0])
    accel_id = q.GetDBDataFrame(query)['accel_id'].values[0]
    
    if q.DoesTableExist('node_alerts') == False:
        #Create a NOAH table if it doesn't exist yet
        create_node_alerts()
        
    node_alert = pos_alert[['disp_alert', 'vel_alert']]
    node_alert['ts'] = end
    node_alert['accel_id'] = accel_id
    q.PushDBDataFrame(node_alert, 'node_alerts', index=False)
        
    query = "SELECT * FROM node_alerts WHERE accel_id = %s and ts >= '%s'" %(accel_id, end-timedelta(hours=3))
    node_alert = q.GetDBDataFrame(query)
    
    node_alert['node_alert'] = np.where(node_alert['vel_alert'].values >= node_alert['disp_alert'].values,

                             #node alert takes the higher perceive risk between vel alert and disp alert
                             node_alert['vel_alert'].values,                                

                             node_alert['disp_alert'].values)
    
    trending_alert = node_alert[node_alert.node_alert > 0]
    trending_alert['id'] = pos_alert['id'].values[0]
    
    try:
        trending_alert['TNL'] = max(trending_alert['node_alert'].values)
    except:
        trending_alert['TNL'] = 0
    
    return trending_alert

def main(pos_alert, tsm_id, end, invalid_nodes):
        
    nodal_pos_alert = pos_alert.groupby('id')
    trending_alert = nodal_pos_alert.apply(trending_alertgen, tsm_id=tsm_id, end=end)
    
    valid_nodes_alert = trending_alert.loc[~trending_alert.id.isin(invalid_nodes)]
    
    try:
        site_alert = max(valid_nodes_alert['TNL'].values)
    except:
        site_alert = 0

    return site_alert