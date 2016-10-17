import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import sys

import rtwindow as rtw
import querySenslopeDb as q
import genproc as g
import AlertAnalysis as A

import RealtimePlotter as plotter

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

def column_alert(alert, num_nodes_to_check, k_ac_ax):

    #DESCRIPTION
    #Evaluates column-level alerts from node alert and velocity data

    #INPUT
    #alert:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts and final node alerts
    #num_nodes_to_check:                integer; number of adjacent nodes to check for validating current node alert
    
    #OUTPUT:
    #alert:                             Pandas DataFrame object; same as input dataframe "alert" with additional column for column-level alert

#    print alert
    col_alert=[]
    col_node=[]
    #looping through each node
    for i in range(1,len(alert)+1):

        if alert['ND'].values[i-1]==0:
            col_node.append(i-1)
            col_alert.append(-1)
    
        #checking if current node alert is 2 or 3
        elif alert['node_alert'].values[i-1]!=0:
            
            #defining indices of adjacent nodes
            adj_node_ind=[]
            for s in range(1,num_nodes_to_check+1):
                if i-s>0: adj_node_ind.append(i-s)
                if i+s<=len(alert): adj_node_ind.append(i+s)

            #looping through adjacent nodes to validate current node alert
            validity_check(adj_node_ind, alert, i, col_node, col_alert, k_ac_ax)
               
        else:
            col_node.append(i-1)
            col_alert.append(alert['node_alert'].values[i-1])
            
    alert['col_alert']=np.asarray(col_alert)

    alert['node_alert']=alert['node_alert'].map({-1:'ND',0:'L0',1:'L2',2:'L3'})
    alert['col_alert']=alert['col_alert'].map({-1:'ND',0:'L0',1:'L2',2:'L3'})

    return alert

def validity_check(adj_node_ind, alert, i, col_node, col_alert, k_ac_ax):

    #DESCRIPTION
    #used in validating current node alert

    #INPUT
    #adj_node_ind                       Indices of adjacent node
    #alert:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts, final node alerts and olumn-level alert
    #i                                  Integer, used for counting
    #col_node                           Integer, current node
    #col_alert                          Integer, current node alert
    #k_ac_ax                            float; minimum value of (minimum velocity / maximum velocity) required to consider movement as valid
    
    #OUTPUT:
    #col_alert, col_node                             

    adj_node_alert=[]
    for j in adj_node_ind:
        if alert['ND'].values[j-1]==0:
            adj_node_alert.append(-1)
        else:
            if alert['vel_alert'].values[i-1]!=0:
                #comparing current adjacent node velocity with current node velocity
                if abs(alert['max_vel'].values[j-1])>=abs(alert['max_vel'].values[i-1])*1/(2.**abs(i-j)):
                    #current adjacent node alert assumes value of current node alert
                    col_node.append(i-1)
                    col_alert.append(alert['node_alert'].values[i-1])
                    break
                    
                else:
                    adj_node_alert.append(0)
                    col_alert.append(max(getmode(adj_node_alert)))
                    break
                
            else:
                check_pl_cur=abs(alert['xz_disp'].values[i-1])>=abs(alert['xy_disp'].values[i-1])

                if check_pl_cur==True:
                    max_disp_cur=abs(alert['xz_disp'].values[i-1])
                    max_disp_adj=abs(alert['xz_disp'].values[j-1])
                else:
                    max_disp_cur=abs(alert['xy_disp'].values[i-1])
                    max_disp_adj=abs(alert['xy_disp'].values[j-1])        

                if max_disp_adj>=max_disp_cur*1/(2.**abs(i-j)):
                    #current adjacent node alert assumes value of current node alert
                    col_node.append(i-1)
                    col_alert.append(alert['node_alert'].values[i-1])
                    break
                    
                else:
                    adj_node_alert.append(0)
                    col_alert.append(max(getmode(adj_node_alert)))
                    break
                
        if j==adj_node_ind[-1]:
            col_alert.append(max(getmode(adj_node_alert)))
        
    return col_alert, col_node

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
    
    query = "SELECT timestamp, site, source, alert FROM senslopedb.%s WHERE site = '%s' ORDER BY timestamp DESC LIMIT 1" %(table_name, df.site.values[0])
    
    df2 = q.GetDBDataFrame(query)
    
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
        query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site LIKE '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(site, window.end)
    else:
        query = "SELECT * FROM ( SELECT * FROM senslopedb.column_level_alert WHERE site = '%s' AND updateTS >= '%s' ORDER BY timestamp DESC) AS sub GROUP BY site" %(site, window.end)
        
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


def main(name=''):
    if name == '':
        name = sys.argv[1].lower()

    start = datetime.now()
    
    window,config = rtw.getwindow()    
    col = q.GetSensorList(name)
    monitoring = g.genproc(col[0], window, config)
    lgd = q.GetLastGoodDataFromDb(monitoring.colprops.name)
    
    
    monitoring_vel = monitoring.vel[window.start:window.end]
    monitoring_vel = monitoring_vel.reset_index().sort_values('ts',ascending=True)
    nodal_dv = monitoring_vel.groupby('id')     
    
    
    alert = nodal_dv.apply(node_alert2, colname=monitoring.colprops.name, num_nodes=monitoring.colprops.nos, T_disp=config.io.t_disp, T_velL2=config.io.t_vell2, T_velL3=config.io.t_vell3, k_ac_ax=config.io.k_ac_ax, lastgooddata=lgd,window=window,config=config)
    alert = column_alert(alert, config.io.num_nodes_to_check, config.io.k_ac_ax)
    
    not_working = q.GetNodeStatus(1).loc[q.GetNodeStatus(1).site == name].node.values
    
    for i in not_working:
        alert = alert.loc[alert.id != i]

    if 'L3' in list(alert.col_alert.values):
        site_alert = 'L3'
    elif 'L2' in list(alert.col_alert.values):
        site_alert = 'L2'
    else:
        site_alert = min(getmode(list(alert.col_alert.values)))
        
    column_level_alert = pd.DataFrame({'timestamp': [window.end], 'site': [monitoring.colprops.name], 'source': ['sensor'], 'alert': [site_alert], 'updateTS': [window.end]})
    
    print column_level_alert
    
    if site_alert in ('L2', 'L3'):
        A.main(monitoring.colprops.name)
    else:
        alert_toDB(column_level_alert, 'column_level_alert', window)
    
    write_site_alert(monitoring.colprops.name, window)

#######################

    query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' and source = 'public' ORDER BY updateTS DESC LIMIT 1" %monitoring.colprops.name[0:3]
    public_alert = q.GetDBDataFrame(query)
    if public_alert.alert.values[0] != 'A0' or RoundTime(pd.to_datetime(public_alert.timestamp.values[0])) == RoundTime(window.end):
        plot_time = ['07:30:00', '19:30:00']
        if str(window.end.time()) in plot_time:
            plotter.main(monitoring, window, config, plotvel_start=window.end-timedelta(hours=3), plotvel_end=window.end)
    elif RoundTime(pd.to_datetime(public_alert.timestamp.values[0])) == RoundTime(window.end):
        plotter.main(monitoring, window, config, plotvel_start=window.end-timedelta(hours=3), plotvel_end=window.end)

#######################

    print 'run time =', datetime.now()-start
    
    return column_level_alert

################################################################################

if __name__ == "__main__":
    main()