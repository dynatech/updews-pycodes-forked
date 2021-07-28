from datetime import timedelta
import numpy as np
import os
import pandas as pd
import sys 

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import dynadb.db as db
import gsm.gsmserver_dewsl3.sms_data as sms

def create_node_alerts():   
    query = "CREATE TABLE `node_alerts` ("
    query += "  `na_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL,"
    query += "  `tsm_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `node_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `disp_alert` TINYINT(1) NOT NULL DEFAULT 0,"
    query += "  `vel_alert` TINYINT(1) NOT NULL DEFAULT 0,"
    query += "  PRIMARY KEY (`na_id`),"
    query += "  UNIQUE INDEX `uq_node_alerts` (`ts` ASC, `tsm_id` ASC, `node_id` ASC),"
    query += "  INDEX `fk_node_alerts_tsm_sensors1_idx` (`tsm_id` ASC),"
    query += "  CONSTRAINT `fk_node_alerts_tsm_sensors1`"
    query += "    FOREIGN KEY (`tsm_id`)"
    query += "    REFERENCES `tsm_sensors` (`tsm_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"

    db.write(query, connection='local')
    
def trending_alert_gen(pos_alert, tsm_id, end):
    
    if qdb.does_table_exist('node_alerts') == False:
        #Create a node_alerts table if it doesn't exist yet
        create_node_alerts()
            
    query = "SELECT EXISTS(SELECT * FROM node_alerts"
    query += " WHERE ts = '%s'" %end
    query += " and tsm_id = %s and node_id = %s)" %(tsm_id, pos_alert['node_id'].values[0])
    
    if db.df_read(query, connection='local').values[0][0] == 0:
        node_alert = pos_alert[['disp_alert', 'vel_alert']]
        node_alert['ts'] = end
        node_alert['tsm_id'] = tsm_id
        node_alert['node_id'] = pos_alert['node_id'].values[0]
        data_table = sms.DataTable('node_alerts', node_alert)
        db.df_write(data_table, connection='local')
    
    query = "SELECT * FROM node_alerts WHERE tsm_id = %s and node_id = %s and ts >= '%s'" %(tsm_id, pos_alert['node_id'].values[0], end-timedelta(hours=3))
    node_alert = db.df_read(query, connection='local')
    
    node_alert['node_alert'] = np.where(node_alert['vel_alert'].values >= node_alert['disp_alert'].values,

                             #node alert takes the higher perceive risk between vel alert and disp alert
                             node_alert['vel_alert'].values,                                

                             node_alert['disp_alert'].values)
    
    if len(node_alert[node_alert.node_alert > 0]) > 3:        
        trending_alert = pd.DataFrame({'node_id': [pos_alert['node_id'].values[0]], 'TNL': [max(node_alert['node_alert'].values)]})
    else:
        trending_alert = pd.DataFrame({'node_id': [pos_alert['node_id'].values[0]], 'TNL': [0]})
    
    return trending_alert

def main(pos_alert, tsm_id, end, invalid_nodes):
        
    nodal_pos_alert = pos_alert.groupby('node_id')
    trending_alert = nodal_pos_alert.apply(trending_alert_gen, tsm_id=tsm_id, end=end)
    
    valid_nodes_alert = trending_alert.loc[~trending_alert.node_id.isin(invalid_nodes)]
    
    try:
        site_alert = max(valid_nodes_alert['TNL'].values)
    except:
        site_alert = 0

    return site_alert
