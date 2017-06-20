import pandas as pd
import numpy as np
from datetime import timedelta
from sqlalchemy import create_engine

import rtwindow as rtw
import querySenslopeDb as q
import genproc as g

def node_alert2(disp_vel, colname, num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,lastgooddata,window,config):
    disp_vel = disp_vel.reset_index(level=1)    
    valid_data = pd.to_datetime(window.end - timedelta(hours=3))
    #initializing DataFrame object, alert
    alert=pd.DataFrame(data=None)

    #adding node IDs
    node_id = disp_vel.id.values[0]
    alert['id']= [node_id]
    alert=alert.set_index('id')

    #checking for nodes with no data
    lastgooddata=lastgooddata.loc[lastgooddata.id == node_id]
#    print "lastgooddata", lastgooddata
    try:
        cond = pd.to_datetime(lastgooddata.ts.values[0]) < valid_data
    except IndexError:
        cond = False
        
    alert['ND']=np.where(cond,
                         
                         #No data within valid date 
                         np.nan,
                         
                         #Data present within valid date
                         np.ones(len(alert)))
    
    #evaluating net displacements within real-time window
    alert['xz_disp']=np.round(disp_vel.xz.values[-1]-disp_vel.xz.values[0], 3)
    alert['xy_disp']=np.round(disp_vel.xy.values[-1]-disp_vel.xy.values[0], 3)

    #determining minimum and maximum displacement
    cond = np.asarray(np.abs(alert['xz_disp'].values)<np.abs(alert['xy_disp'].values))
    min_disp=np.round(np.where(cond,
                               np.abs(alert['xz_disp'].values),
                               np.abs(alert['xy_disp'].values)), 4)
    cond = np.asarray(np.abs(alert['xz_disp'].values)>=np.abs(alert['xy_disp'].values))
    max_disp=np.round(np.where(cond,
                               np.abs(alert['xz_disp'].values),
                               np.abs(alert['xy_disp'].values)), 4)

    #checking if displacement threshold is exceeded in either axis    
    cond = np.asarray((np.abs(alert['xz_disp'].values)>T_disp, np.abs(alert['xy_disp'].values)>T_disp))
    alert['disp_alert']=np.where(np.any(cond, axis=0),

                                 #disp alert=2
                                 np.where(min_disp/max_disp<k_ac_ax,
                                          np.zeros(len(alert)),
                                          np.ones(len(alert))),

                                 #disp alert=0
                                 np.zeros(len(alert)))
    
    #getting minimum axis velocity value
    alert['min_vel']=np.round(np.where(np.abs(disp_vel.vel_xz.values[-1])<np.abs(disp_vel.vel_xy.values[-1]),
                                       np.abs(disp_vel.vel_xz.values[-1]),
                                       np.abs(disp_vel.vel_xy.values[-1])), 4)

    #getting maximum axis velocity value
    alert['max_vel']=np.round(np.where(np.abs(disp_vel.vel_xz.values[-1])>=np.abs(disp_vel.vel_xy.values[-1]),
                                       np.abs(disp_vel.vel_xz.values[-1]),
                                       np.abs(disp_vel.vel_xy.values[-1])), 4)
                                       
    #checking if proportional velocity is present across node
    alert['vel_alert']=np.where(alert['min_vel'].values/alert['max_vel'].values<k_ac_ax,   

                                #vel alert=0
                                np.zeros(len(alert)),    

                                #checking if max node velocity exceeds threshold velocity for alert 1
                                np.where(alert['max_vel'].values<=T_velL2,                  

                                         #vel alert=0
                                         np.zeros(len(alert)),

                                         #checking if max node velocity exceeds threshold velocity for alert 2
                                         np.where(alert['max_vel'].values<=T_velL3,         

                                                  #vel alert=1
                                                  np.ones(len(alert)),

                                                  #vel alert=2
                                                  np.ones(len(alert))*2)))
    
    alert['node_alert']=np.where(alert['vel_alert'].values >= alert['disp_alert'].values,

                                 #node alert takes the higher perceive risk between vel alert and disp alert
                                 alert['vel_alert'].values,                                

                                 alert['disp_alert'].values)


    alert['disp_alert']=alert['ND']*alert['disp_alert']
    alert['vel_alert']=alert['ND']*alert['vel_alert']
    alert['node_alert']=alert['ND']*alert['node_alert']
    alert['ND']=alert['ND'].map({0:1,1:1})          #para saan??
    alert['ND']=alert['ND'].fillna(value=0)         #para saan??
    alert['disp_alert']=alert['disp_alert'].fillna(value=-1)
    alert['vel_alert']=alert['vel_alert'].fillna(value=-1)
    alert['node_alert']=alert['node_alert'].fillna(value=-1)
    
    alert=alert.reset_index()
 
    return alert

def column_alert(col_alert, alert, num_nodes_to_check, k_ac_ax, T_velL2, T_velL3):

    #DESCRIPTION
    #Evaluates column-level alerts from node alert and velocity data

    #INPUT
    #alert:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts and final node alerts
    #num_nodes_to_check:                integer; number of adjacent nodes to check for validating current node alert
    
    #OUTPUT:
    #alert:                             Pandas DataFrame object; same as input dataframe "alert" with additional column for column-level alert

    i = col_alert['id'].values[0]
    #checking if current node alert is 2 or 3
    if alert[alert.id == i]['node_alert'].values[0] != 0:
 
        #defining indices of adjacent nodes
        adj_node_ind=[]
        for s in range(1,num_nodes_to_check+1):
            if i-s>0: adj_node_ind.append(i-s)
            if i+s<=len(alert): adj_node_ind.append(i+s)

        #looping through adjacent nodes to validate current node alert
        validity_check(adj_node_ind, alert, i, T_velL2, T_velL3)
           
    else:
        alert.loc[alert.id == i, 'col_alert'] = alert[alert.id == i]['node_alert'].values[0]

def validity_check(adj_node_ind, alert, i, T_velL2, T_velL3):

    #DESCRIPTION
    #used in validating current node alert

    #INPUT
    #adj_node_ind                       Indices of adjacent node
    #alert:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts, final node alerts and olumn-level alert
    #i                                  Integer, used for counting
    #col_node                           Integer, current node
    #col_alert                          Integer, current node alert
    
    #OUTPUT:
    #col_alert, col_node                             

    if alert[alert.id == i]['disp_alert'].values[0] == alert[alert.id == i]['vel_alert'].values[0] or alert[alert.id == i]['vel_alert'].values[0] <= 0:
        alert.loc[alert.id == i, 'col_alert'] = alert[alert.id == i]['node_alert'].values[0]

    else:
        for j in adj_node_ind:
            if alert[alert.id == j]['ND'].values[0]==0:
                continue
            else:
                #comparing current adjacent node velocity with current node velocity
                if abs(alert[alert.id == j]['max_vel'].values[0])>=abs(alert[alert.id == i]['max_vel'].values[0])*1/(2.**abs(i-j)):
                    #current adjacent node alert assumes value of current node alert
                    alert.loc[alert.id == i, 'col_alert'] = alert[alert.id == i]['node_alert'].values[0]
                    break
                
                elif alert[alert.id == i]['min_vel'].values[0] >= T_velL3 and abs(alert[alert.id == j]['max_vel'].values[0])>=abs(alert[alert.id == i]['min_vel'].values[0])*1/(2.**abs(i-j)):
                    alert.loc[alert.id == i, 'col_alert'] = 2
                    break

                elif alert[alert.id == i]['min_vel'].values[0] >= T_velL2 and abs(alert[alert.id == j]['max_vel'].values[0])>=abs(alert[alert.id == i]['min_vel'].values[0])*1/(2.**abs(i-j)):
                    alert.loc[alert.id == i, 'col_alert'] = 1
                    break

                elif alert['disp_alert'].values[i-1] > 0:
                    alert.loc[alert.id == i, 'col_alert'] = alert[alert.id == i]['disp_alert'].values[0]
                    break

                else:
                    alert.loc[alert.id == i, 'col_alert'] = 0
                    break

            if j==adj_node_ind[-1]:
                alert.loc[alert.id == i, 'col_alert'] = -1

def getmode(li):
    li.sort()
    numbers = {}
    for x in li:
        num = li.count(x)
        numbers[x] = num
    highest = max(numbers.values())
    n = []
    for m in numbers.keys():
        if numbers[m] == highest:
            n.append(m)
    return n

def trending_alertgen(trending_alert, monitoring, lgd, window, config):
    endTS = pd.to_datetime(trending_alert['timestamp'].values[0])
    monitoring_vel = monitoring.vel[endTS-timedelta(3):endTS]
    monitoring_vel = monitoring_vel.reset_index().sort_values('ts',ascending=True)
    nodal_dv = monitoring_vel.groupby('id', as_index=False)     
    
    alert = nodal_dv.apply(node_alert2, colname=monitoring.colprops.name, num_nodes=monitoring.colprops.nos, T_disp=config.io.t_disp, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3, k_ac_ax=config.io.k_ac_ax, lastgooddata=lgd,window=window,config=config)
    alert = alert.reset_index(drop=True)
    alert['col_alert'] = -1
    col_alert = pd.DataFrame({'id': range(1, monitoring.colprops.nos+1), 'col_alert': [-1]*monitoring.colprops.nos})
    node_col_alert = col_alert.groupby('id', as_index=False)
    node_col_alert.apply(column_alert, alert=alert, num_nodes_to_check=config.io.num_nodes_to_check, k_ac_ax=config.io.k_ac_ax, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3)

    alert['node_alert']=alert['node_alert'].map({-1:'ND',0:'L0',1:'L2',2:'L3'})
    alert['col_alert']=alert['col_alert'].map({-1:'ND',0:'L0',1:'L2',2:'L3'})

    alert['timestamp']=endTS
    
    palert = alert.loc[(alert.col_alert == 'L2') | (alert.col_alert == 'L3')]

    if len(palert) != 0:
        palert['site']=monitoring.colprops.name
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
            query = "SELECT * FROM senslopedb.node_level_alert WHERE site = '%s' and timestamp >= '%s' and timestamp <= '%s' and id = %s" %(monitoring.colprops.name, endTS-timedelta(hours=3), endTS, i)
            nodal_palertDF = q.GetDBDataFrame(query)
            if len(nodal_palertDF) > 3:
                palert_index = alert.loc[alert.id == i].index[0]
                alert.loc[palert_index, ['TNL']] = max(getmode(list(nodal_palertDF['col_alert'].values)))
            else:
                palert_index = alert.loc[alert.id == i].index[0]
                alert.loc[palert_index, ['TNL']] = 'L0'
    
    not_working = q.GetNodeStatus(1).loc[q.GetNodeStatus(1).site == monitoring.colprops.name]['node'].values
    
    for i in not_working:
        alert = alert.loc[alert.id != i]
    
    if 'L3' in alert['TNL'].values:
        site_alert = 'L3'
    elif 'L2' in alert['TNL'].values:
        site_alert = 'L2'
    else:
        site_alert = min(getmode(list(alert['TNL'].values)))
    
    alert_index = trending_alert.loc[trending_alert.timestamp == endTS].index[0]
    trending_alert.loc[alert_index] = [endTS, monitoring.colprops.name, 'sensor', site_alert]
    
    return trending_alert

def main(site, end):
        
    window,config = rtw.getwindow(end)
    
    monwinTS = pd.date_range(start = window.end - timedelta(hours=3), end = window.end, freq = '30Min')
    trending_alert = pd.DataFrame({'site': [np.nan]*len(monwinTS), 'alert': [np.nan]*len(monwinTS), 'timestamp': monwinTS, 'source': [np.nan]*len(monwinTS)})
    trending_alert = trending_alert[['timestamp', 'site', 'source', 'alert']]
    
    col = q.GetSensorList(site)
    
    monitoring = g.genproc(col[0], window, config, config.io.column_fix)
    lgd = q.GetLastGoodDataFromDb(monitoring.colprops.name)

    
    trending_alertTS = trending_alert.groupby('timestamp', as_index=False)
    output = trending_alertTS.apply(trending_alertgen, window=window, config=config, monitoring=monitoring, lgd=lgd)
    output = output.reset_index(drop=True)
    
    site_level_alert = output.loc[output.timestamp == window.end]
    site_level_alert['updateTS'] = [window.end]
    
    return site_level_alert