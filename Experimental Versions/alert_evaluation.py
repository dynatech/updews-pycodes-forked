#DESCRIPTION:
#module for evaluating column alerts


from datetime import datetime, date, time, timedelta
import numpy as np
import pandas as pd
import ConfigParser
import generic_functions as gf

cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')

#time interval between data points, in hours
data_dt=0.5

#length of real-time monitoring window, in days
rt_window_length=3.

#length of rolling/moving window operations in hours
roll_window_length=3.

#number of rolling window operations in the whole monitoring analysis
num_roll_window_ops=2


#local file paths
columnproperties_path=cfg.get('I/O','ColumnProperties')
purged_path=cfg.get('I/O','InputFilePath')
monitoring_path=cfg.get('I/O','MonitoringPath')
LastGoodData_path=cfg.get('I/O','LastGoodData')
proc_monitoring_path=cfg.get('I/O','OutputFilePathMonitoring2')

#file names
columnproperties_file='column_properties.csv'
purged_file='.csv'
monitoring_file='.csv'
LastGoodData_file='.csv'
proc_monitoring_file='.csv'

#file headers
columnproperties_headers=['colname','num_nodes','seg_len']
purged_file_headers=['ts','id','x', 'y', 'z', 'm']
monitoring_file_headers=['ts','id','x', 'y', 'z', 'm']
LastGoodData_file_headers=['ts','id','x', 'y', 'z', 'm']
proc_monitoring_file_headers=['ts','id','xz', 'xy', 'm']
colarrange=['colname', 'node_ID', 'ND', 'xz_disp', 'xy_disp', 'disp_alert', 'min_vel', 'max_vel', 'vel_alert', 'node_alert']

roll_window_numpts=int(1+roll_window_length/data_dt)
end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
valid_data = end - timedelta(days=1)

def node_alert(colname, xz_tilt, xy_tilt, xz_vel, xy_vel, num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax):

    #DESCRIPTION
    #Evaluates node-level alerts from node tilt and velocity data

    #INPUT
    #xz_tilt,xy_tilt, xz_vel, xy_vel:   Pandas DataFrame objects, with length equal to real-time window size, and columns for timestamp and individual node values
    #num_nodes:                         integer; number of nodes in a column
    #T_disp, TvelA1, TvelA2:            floats; threshold values for displacement, and velocities correspoding to alert levels A1 and A2
    #k_ac_ax:                           float; minimum value of (minimum velocity / maximum velocity) required to consider movement as valid

    #OUTPUT:
    #alert:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts and final node alerts

    #print xz_tilt
    #print xy_tilt
    #print xz_vel
    #print xy_vel

    #initializing DataFrame object, alert
      
    alert=pd.DataFrame(data=None)

    #adding column name and its node ids
    index=[]
    for x in range(1, num_nodes+1):
        index.append(colname)  
    alert['colname']=index
    alert['node_ID']=[num_nodes-a for a in range(num_nodes)]

    #checking for nodes with no data
    alert=alert.set_index('node_ID')
    LastGoodData=pd.read_csv(LastGoodData_path+colname+LastGoodData_file,names=LastGoodData_file_headers,parse_dates=[0],index_col=[1])
    LastGoodData=LastGoodData[:num_nodes]
    cond = np.asarray((LastGoodData.ts<valid_data))
    cond = cond[::-1]
    alert['ND']=np.where(cond,
                         
                         #No data within valid date 
                         np.nan,
                         
                         #Data present within valid date
                         np.ones(len(alert)))

    #evaluating net displacements within real-time window
    alert['xz_disp']=np.round(xz_tilt.values[-1]-xz_tilt.values[13], 3)
    alert['xy_disp']=np.round(xy_tilt.values[-1]-xy_tilt.values[13], 3)

    #checking if displacement threshold is exceeded in either axis
    cond = np.asarray((np.abs(alert['xz_disp'].values)>T_disp, np.abs(alert['xy_disp'].values)>T_disp))
    alert['disp_alert']=np.where(np.any(cond, axis=0),

                                 #disp alert=1
                                 np.ones(len(alert)),

                                 #disp alert=0
                                 np.zeros(len(alert)))

    #getting minimum axis velocity value
    alert['min_vel']=np.round(np.where(np.abs(xz_vel.values[-1])<np.abs(xy_vel.values[-1]),
                                       np.abs(xz_vel.values[-1]),
                                       np.abs(xy_vel.values[-1])), 4)

    #getting maximum axis velocity value
    alert['max_vel']=np.round(np.where(np.abs(xz_vel.values[-1])>=np.abs(xy_vel.values[-1]),
                                       np.abs(xz_vel.values[-1]),
                                       np.abs(xy_vel.values[-1])), 4)
                                
    #checking if proportional velocity is present across node
    alert['vel_alert']=np.where(alert['min_vel'].values/alert['max_vel'].values<k_ac_ax,   

                                #vel alert=0
                                np.zeros(len(alert)),    

                                #checking if max node velocity exceeds threshold velocity for alert 1
                                np.where(alert['max_vel'].values<=T_velA1,                  

                                         #vel alert=0
                                         np.zeros(len(alert)),

                                         #checking if max node velocity exceeds threshold velocity for alert 2
                                         np.where(alert['max_vel'].values<=T_velA2,         

                                                  #vel alert=1
                                                  np.ones(len(alert)),

                                                  #vel alert=2
                                                  2*np.ones(len(alert)))))

    
    alert['node_alert']=np.where(alert['vel_alert'].values==0,

                                 # node alert = displacement alert (0 or 1) if velocity alert is 0 
                                 alert['disp_alert'].values,                                

                                 # node alert = velocity alert if displacement alert = 1 
                                 alert['disp_alert'].values*alert['vel_alert'].values)


    #rearrange columns
    alert=alert.reset_index()
    cols=alert.columns.tolist()
    cols=colarrange
    alert = alert[cols]
    
    return alert

def column_alert(alert, num_nodes_to_check):

    #DESCRIPTION
    #Evaluates column-level alerts from node alert and velocity data

    #INPUT
    #alert:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts and final node alerts
    #num_nodes_to_check:                integer; number of adjacent nodes to check for validating current node alert
    
    #OUTPUT:
    #alert:                             Pandas DataFrame object; same as input dataframe "alert" with additional column for column-level alert

       
    
    col_alert=[]
    col_node=[]
    #looping through each node
    for i in range(1,len(alert)+1):
    
        #checking if current node alert is 1 or 2
        if alert['node_alert'].values[i-1]>0:
            
            #defining indices of adjacent nodes
            adj_node_ind=[]
            for s in range(1,num_nodes_to_check+1):
                if i-s>0: adj_node_ind.append(i-s)
                if i+s<=len(alert):adj_node_ind.append(i+s)
            
            #looping through adjacent nodes to validate current node alert
            adj_node_alert=[]
            for j in adj_node_ind:
                
                #comparing current adjacent node velocity with current node velocity
                if abs(alert['max_vel'].values[j-1])>=abs(alert['max_vel'].values[i-1])*1/(2.**abs(s)):
                    #current adjacent node alert assumes value of current node alert
                    adj_node_alert.append(alert['node_alert'].values[i-1])
                else:
                    #current adjacent node alert is 0
                    adj_node_alert.append(0)
                    
            #appending validated current node alert to column alert
            col_node.append(i-1)
            col_alert.append(max(adj_node_alert))
            
        else:
            col_node.append(i-1)
            col_alert.append(alert['node_alert'].values[i-1])
        
    alert['col_alert']=np.asarray(col_alert)

    #propagates nd to column alert
    alert['col_alert']=alert['ND']*alert['col_alert']
    alert['ND']=alert['ND'].fillna(value='nd')
    alert['col_alert']=alert['col_alert'].fillna(value='nd')
    
    return alert
            



