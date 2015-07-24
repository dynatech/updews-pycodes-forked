#DESCRIPTION:
#module for evaluating column alerts


from datetime import datetime, date, time, timedelta
import numpy as np
import pandas as pd
import ConfigParser
import generic_functions as gf

from querySenslopeDb import *
from filterSensorData import *

cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')

##set/get values from config file

#time interval between data points, in hours
data_dt = cfg.getfloat('I/O','data_dt')

#length of real-time monitoring window, in days
rt_window_length = cfg.getfloat('I/O','rt_window_length')

#length of rolling/moving window operations in hours
roll_window_length = cfg.getfloat('I/O','roll_window_length')

#number of rolling window operations in the whole monitoring analysis
num_roll_window_ops = cfg.getfloat('I/O','num_roll_window_ops')

#INPUT/OUTPUT FILES

#local file paths
columnproperties_path = cfg.get('I/O','ColumnPropertiesPath')
purged_path = cfg.get('I/O','InputFilePath')
monitoring_path = cfg.get('I/O','MonitoringPath')
LastGoodData_path = cfg.get('I/O','LastGoodData')
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring2')

#file names
columnproperties_file = cfg.get('I/O','ColumnProperties')
purged_file = cfg.get('I/O','CSVFormat')
monitoring_file = cfg.get('I/O','CSVFormat')
LastGoodData_file = cfg.get('I/O','CSVFormat')
proc_monitoring_file = cfg.get('I/O','CSVFormat')

#file headers
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')
purged_file_headers = cfg.get('I/O','purged_file_headers').split(',')
monitoring_file_headers = cfg.get('I/O','monitoring_file_headers').split(',')
LastGoodData_file_headers = cfg.get('I/O','LastGoodData_file_headers').split(',')
proc_monitoring_file_headers = cfg.get('I/O','proc_monitoring_file_headers').split(',')
colarrange = cfg.get('I/O','alerteval_colarrange').split(',')



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

    

    #initializing DataFrame object, alert
    alert=pd.DataFrame(data=None)

    #adding node IDs
    alert['id']=[n for n in range(1,1+num_nodes)]
    alert=alert.set_index('id')

    #checking for nodes with no data
    LastGoodData= GetLastGoodDataFromDb(colname)
    LastGoodData=LastGoodData[:num_nodes]
    cond = np.asarray((LastGoodData.ts<valid_data))
    if len(LastGoodData)<num_nodes:
        x=np.ones(num_nodes-len(LastGoodData),dtype=bool)
        cond=np.append(cond,x)
    alert['ND']=np.where(cond,
                         
                         #No data within valid date 
                         np.zeros(len(alert)),
                         
                         #Data present within valid date
                         np.ones(len(alert)))
    
    #evaluating net displacements within real-time window
    alert['xz_disp']=np.round(xz_tilt.values[-1]-xz_tilt.values[0], 3)
    alert['xy_disp']=np.round(xy_tilt.values[-1]-xy_tilt.values[0], 3)
    
    #checking if displacement threshold is exceeded in either axis
    cond = np.asarray((np.abs(alert['xz_disp'].values)>T_disp, np.abs(alert['xy_disp'].values)>T_disp))
    alert['disp_alert']=np.where(np.any(cond, axis=0),

                                 #disp alert=1
                                 np.ones(len(alert)),

                                 #disp alert=a0
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
                                                  np.zeros(len(alert)))))
    
    alert['node_alert']=np.where(alert['vel_alert'].values==0,

                                 # node alert = displacement alert (0 or 1) if velocity alert is a0 
                                 alert['disp_alert'].values,                                

                                 # node alert = velocity alert if displacement alert = 1 
                                 np.where(alert['disp_alert'].values==1,
                                          alert['vel_alert'].values,
                                          alert['disp_alert'].values))


    #rearrange columns
    alert=alert.reset_index()
    cols=colarrange
    alert = alert[cols]

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

       

    col_alert=[]
    col_node=[]
    #looping through each node
    for i in range(1,len(alert)+1):
    
        #checking if current node alert is 1 or 2
        if alert['node_alert'].values[i-1]!=0:
            
            #defining indices of adjacent nodes
            adj_node_ind=[]
            for s in range(1,num_nodes_to_check+1):
                if i-s>0: adj_node_ind.append(i-s)
                if i+s<=len(alert): adj_node_ind.append(i+s)
            
            #looping through adjacent nodes to validate current node alert
            adj_node_alert=[]
            for j in adj_node_ind:

                #checking if adjacent node alert has alert, else 
                if alert['node_alert'].values[j-1]!=0:
                    #proceeding if data is available within set valid date
                    if alert['ND'].values[j-1]!=0:
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
                
                    continue
                
                else:
                    col_node.append(i-1)
                    col_alert.append(0)
                    break

        else:
            col_node.append(i-1)
            if alert['ND'].values[i-1]==0:
                col_alert.append(-1)
            else:
                col_alert.append(alert['node_alert'].values[i-1])

    alert['col_alert']=np.asarray(col_alert)

    alert['node_alert']=alert['node_alert'].map({0:'a0',1:'a1',2:'a2'})
    alert['col_alert']=alert['col_alert'].map({-1:'nd',0:'a0',1:'a1',2:'a2'})

    return alert
            



