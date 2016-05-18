#DESCRIPTION:
#module for evaluating column alerts


from datetime import datetime, date, time, timedelta
import numpy as np
import pandas as pd
import ConfigParser
import os
import sys

import generic_functions as gf

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

from querySenslopeDb import *
from filterSensorData import *

#Function for directory manipulations
def up_one(p):
    out = os.path.abspath(os.path.join(p, '..'))
    return out

cfg = ConfigParser.ConfigParser()
cfg.read(up_one(os.path.dirname(__file__))+'/server-config.txt')

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

#file headers
colarrange = cfg.get('I/O','alerteval_colarrange').split(',')
TestSpecificTime = cfg.getboolean('I/O', 'test_specific_time')



if TestSpecificTime:
    end = pd.to_datetime(cfg.get('I/O','use_specific_time'))
else:
    end = datetime.now()


roll_window_numpts=int(1+roll_window_length/data_dt)
end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops,end)
valid_data = end - timedelta(hours=3)



def node_alert(colname, xz_tilt, xy_tilt, xz_vel, xy_vel, num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax):

    #DESCRIPTION
    #Evaluates node-level alerts from node tilt and velocity data

    #INPUT
    #xz_tilt,xy_tilt, xz_vel, xy_vel:   Pandas DataFrame objects, with length equal to real-time window size, and columns for timestamp and individual node values
    #num_nodes:                         integer; number of nodes in a column
    #T_disp, TvelL2, TvelL3:            floats; threshold values for displacement, and velocities correspoding to alert levels L2 and L3
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
                         np.nan,
                         
                         #Data present within valid date
                         np.ones(len(alert)))
    
    #evaluating net displacements within real-time window
    alert['xz_disp']=np.round(xz_tilt.values[-1]-xz_tilt.values[0], 3)
    alert['xy_disp']=np.round(xy_tilt.values[-1]-xy_tilt.values[0], 3)

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
                                np.where(alert['max_vel'].values<=T_velL2,                  

                                         #vel alert=0
                                         np.zeros(len(alert)),

                                         #checking if max node velocity exceeds threshold velocity for alert 2
                                         np.where(alert['max_vel'].values<=T_velL3,         

                                                  #vel alert=2
                                                  np.ones(len(alert)),

                                                  #vel alert=3
                                                  np.ones(len(alert))*2)))
    
    alert['node_alert']=np.where(alert['vel_alert'].values==0,

                                 # node alert = displacement alert (0 or 2) if velocity alert is 0 
                                 alert['disp_alert'].values,                                

                                 # node alert = velocity alert if displacement alert = 2 
                                 np.where(alert['disp_alert'].values==1,
                                          alert['disp_alert'].values,
                                          alert['vel_alert'].values))


#    alert['ND']=alert['ND']*alert['disp_alert']
    alert['disp_alert']=alert['ND']*alert['disp_alert']
    alert['vel_alert']=alert['ND']*alert['vel_alert']
    alert['node_alert']=alert['ND']*alert['node_alert']
    alert['ND']=alert['ND'].map({0:1,1:1})
    alert['ND']=alert['ND'].fillna(value=0)
    alert['disp_alert']=alert['disp_alert'].fillna(value=-1)
    alert['vel_alert']=alert['vel_alert'].fillna(value=-1)
    alert['node_alert']=alert['node_alert'].fillna(value=-1)

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
                    col_alert.append(max(gf.getmode(adj_node_alert)))
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
                    col_alert.append(max(gf.getmode(adj_node_alert)))
                    break
                
        if j==adj_node_ind[-1]:
            col_alert.append(max(gf.getmode(adj_node_alert)))
        
    return col_alert, col_node