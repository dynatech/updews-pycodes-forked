import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import sys

import rtwindow as rtw
import querySenslopeDb as q
import genproc_netvel as g
import AlertAnalysis_netvel as A

def RoundTime(date_time):
    # rounds time to 4/8/12 AM/PM
    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4
    if quotient == 5:
        date_time = pd.to_datetime(date_time.date() + timedelta(1))
    else:
        time = (quotient+1)*4
        date_time = pd.to_datetime(date_time.date()) + timedelta(hours=time)
            
    return date_time

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

    try:
        cond = pd.to_datetime(lastgooddata.ts.values[0]) < valid_data
    except IndexError:
        cond = True
        
    alert['ND']=np.where(cond,
                         
                         #No data within valid date 
                         np.nan,
                         
                         #Data present within valid date
                         np.ones(len(alert)))
    
    #evaluating net displacements within real-time window
    alert['disp']=np.round(disp_vel.net_dist.values[-1]-disp_vel.net_dist.values[0], 3)

    #checking if displacement threshold is exceeded in either axis    
    cond = np.asarray(np.abs(alert['disp'].values)>T_disp)
    alert['disp_alert']=np.where(np.any(cond, axis=0),

                                 #disp alert=2
                                 np.ones(len(alert))*2,

                                 #disp alert=0
                                 np.zeros(len(alert)))
    
    alert['net_vel'] = np.round(disp_vel.net_vel.values[-1])
    #checking if proportional velocity is present across node
    alert['vel_alert']=np.where(np.abs(alert['net_vel'].values)<=T_velL2,                  

                                 #vel alert=0
                                 np.zeros(len(alert)),

                                 #checking if max node velocity exceeds threshold velocity for alert 2
                                 np.where(np.abs(alert['net_vel'].values)<=T_velL3,         

                                          #vel alert=2
                                          np.ones(len(alert))*2,

                                          #vel alert=3
                                          np.ones(len(alert))*3))
    
    alert['node_alert']=np.where(alert['vel_alert'].values >= alert['disp_alert'].values,

                                 #node alert takes the higher perceive risk between vel alert and disp alert
                                 alert['vel_alert'].values,                                

                                 alert['disp_alert'].values)


    alert['disp_alert']=alert['ND']*alert['disp_alert']
    alert['vel_alert']=alert['ND']*alert['vel_alert']
    alert['node_alert']=alert['ND']*alert['node_alert']
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
    if alert[alert.id == i]['node_alert'].values[0] > 0:
 
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
                if abs(alert[alert.id == j]['net_vel'].values[0])>=abs(alert[alert.id == i]['net_vel'].values[0])*1/(2.**abs(i-j)):
                    #current adjacent node alert assumes value of current node alert
                    alert.loc[alert.id == i, 'col_alert'] = alert[alert.id == i]['node_alert'].values[0]
                    break
                
                elif alert[alert.id == i]['net_vel'].values[0] >= T_velL3 and abs(alert[alert.id == j]['net_vel'].values[0])>=abs(alert[alert.id == i]['net_vel'].values[0])*1/(2.**abs(i-j)):
                    alert.loc[alert.id == i, 'col_alert'] = 3
                    break

                elif alert[alert.id == i]['net_vel'].values[0] >= T_velL2 and abs(alert[alert.id == j]['net_vel'].values[0])>=abs(alert[alert.id == i]['net_vel'].values[0])*1/(2.**abs(i-j)):
                    alert.loc[alert.id == i, 'col_alert'] = 2
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

def alert_toDB(df, table_name, window):
    
    query = "SELECT * FROM %s WHERE site = '%s' and source = 'netvel' and timestamp <= '%s' AND updateTS >= '%s' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0], window.end, window.end-timedelta(hours=0.5))
    
    try:
        df2 = q.GetDBDataFrame(query)
    except:
        df2 = pd.DataFrame()        

    try:
        same_alert = df2['alert'].values[0] == df['alert'].values[0]
    except:
        same_alert = False

    query = "SELECT EXISTS(SELECT * FROM %s" %table_name
    query += " WHERE timestamp = '%s' AND site = '%s'" %(pd.to_datetime(df['updateTS'].values[0]), df['site'].values[0])
    if table_name == 'site_level_alert':
        query += " AND source = 'netvel'"
    query += ")"
    if q.GetDBDataFrame(query).values[0][0] == 1:
        inDB = True
    else:
        inDB = False

    if (len(df2) == 0 or not same_alert) and not inDB:
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        
    elif same_alert and df2['updateTS'].values[0] < df['updateTS'].values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.%s SET updateTS='%s' WHERE site = '%s' and source = 'netvel' and alert = '%s' and timestamp = '%s'" %(table_name, window.end, df2.site.values[0], df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
        cur.execute(query)
        db.commit()
        db.close()

def write_site_alert(site, window):
    if site != 'messb' and site != 'mesta':
        site = site[0:3] + '%'
        query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert"
        query += " WHERE site LIKE '%s' and timestamp <= '%s'" %(site, window.end)
        query += " AND updateTS >= '%s' ORDER BY timestamp DESC" %window.end
        query += ") AS sub GROUP BY site"
    else:
        query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert"
        query += " WHERE site = '%s' and timestamp <= '%s'" %(site, window.end)
        query += " AND updateTS >= '%s' ORDER BY timestamp DESC" %window.end
        query += ") AS sub GROUP BY site"
        
    df = q.GetDBDataFrame(query)

    if 'V3' in list(df.alert.values):
        site_alert = 'V3'
    elif 'V2' in list(df.alert.values):
        site_alert = 'V2'
    elif 'V0' in list(df.alert.values):
        site_alert = 'V0'
    else:
        site_alert = 'VN'
        
    if site == 'messb':
        site = 'msl'
    if site == 'mesta':
        site = 'msu'
        
    output = pd.DataFrame({'timestamp': [window.end], 'site': [site[0:3]], 'source': ['netvel'], 'alert': [site_alert], 'updateTS': [window.end]})
    
    alert_toDB(output, 'site_level_alert', window)
    
    return output


def main(name='', end='', end_mon=False):
    start = datetime.now()
    print start

    if name == '':
        name = sys.argv[1].lower()

    if end == '':
        try:
            end = pd.to_datetime(sys.argv[2])
            if end > start + timedelta(hours=0.5):
                print 'invalid timestamp'
                return
        except:
            end = datetime.now()
    else:
        end = pd.to_datetime(end)
    
    window,config = rtw.getwindow(end)

    col = q.GetSensorList(name)
    monitoring = g.genproc(col[0], window, config, config.io.column_fix)
    lgd = q.GetLastGoodDataFromDb(monitoring.colprops.name)
    
    
    monitoring_vel = monitoring.disp_vel[window.start:window.end]
    monitoring_vel = monitoring_vel.reset_index().sort_values('ts',ascending=True)
    nodal_dv = monitoring_vel.groupby('id')     
    
    alert = nodal_dv.apply(node_alert2, colname=monitoring.colprops.name, num_nodes=monitoring.colprops.nos, T_disp=config.io.t_disp, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3, k_ac_ax=config.io.k_ac_ax, lastgooddata=lgd,window=window,config=config)
    alert['col_alert'] = -1
    col_alert = pd.DataFrame({'id': range(1, monitoring.colprops.nos+1), 'col_alert': [-1]*monitoring.colprops.nos})
    node_col_alert = col_alert.groupby('id', as_index=False)
    node_col_alert.apply(column_alert, alert=alert, num_nodes_to_check=config.io.num_nodes_to_check, k_ac_ax=config.io.k_ac_ax, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3)

    alert['node_alert']=alert['node_alert'].map({-1:'VN',0:'V0',2:'V2',3:'V3'})
    alert['col_alert']=alert['col_alert'].map({-1:'VN',0:'V0',2:'V2',3:'V3'})

    not_working = q.GetNodeStatus(1).loc[q.GetNodeStatus(1).site == name].node.values
    
    for i in not_working:
        alert = alert.loc[alert.id != i]

    if 'V3' in list(alert.col_alert.values):
        site_alert = 'V3'
    elif 'V2' in list(alert.col_alert.values):
        site_alert = 'V2'
    else:
        site_alert = min(getmode(list(alert.col_alert.values)))
        
    column_level_alert = pd.DataFrame({'timestamp': [window.end], 'site': [monitoring.colprops.name], 'source': ['netvel'], 'alert': [site_alert], 'updateTS': [window.end]})

    if site_alert in ('V2', 'V3'):
        column_level_alert = A.main(monitoring.colprops.name, window.end)

    alert_toDB(column_level_alert, 'column_level_alert', window)
        
    write_site_alert(monitoring.colprops.name, window)

    print column_level_alert

    return column_level_alert

################################################################################

def alert_regen(df, ts):
    col = df['name'].values[0]
    query = "SELECT max(timestamp) FROM %s " %col
    data_ts = q.GetDBDataFrame(query).values[0][0]
    if data_ts == None:
        pass
    if pd.to_datetime(data_ts) >= ts:
        main(col, ts)            

def recent_end_ts(df, month):
    site = df['site'].values[0]
    query = "SELECT max(updateTS) FROM site_level_alert "
    query += "where site like '%s' " %(site+'%')
    query += "and source = 'netvel'"
    query += "and timestamp < '2017-%s-01'" %(month+1)
    end_ts = q.GetDBDataFrame(query).values[0][0]
    if end_ts == None:
        end_ts = '2017-%s-01' %month
    end_ts = pd.to_datetime(end_ts)
    
    ts = end_ts
    while ts < pd.to_datetime('2017-%s-01' %(month+1)): 
        col_df = df.groupby('name', as_index=False)
        col_df.apply(alert_regen, ts=ts)
        ts += timedelta(hours=0.5)

def main_regen(month='', site_ratio_start='', site_ratio_end=''):
    
    if month == '':
        month = int(sys.argv[1])

    if site_ratio_start == '':
        try:
            site_ratio_start = int(sys.argv[2])
        except:
            site_ratio_start = ''

    if site_ratio_end == '':
        try:
            site_ratio_end = int(sys.argv[3])
        except:
            site_ratio_end = ''

    query = "SELECT name, date_activation FROM site_column"
    query += " ORDER BY name"
    df = q.GetDBDataFrame(query)
    
    df = df[['name']]
    df['site'] = df['name'].apply(lambda x: x[0:3])
    
    if site_ratio_end == '':
        site_ratio_end = len(set(df.site))
        
    if site_ratio_start == '':
        site_ratio_start = 0
        
    df = df[df.site.isin(sorted(set(df.site))[site_ratio_start:site_ratio_end])]
    
    site_df = df.groupby('site', as_index=False)
    site_df.apply(recent_end_ts, month=month)


################################################################################

if __name__ == "__main__":        
    main_regen(4)