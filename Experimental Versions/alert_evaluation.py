#DESCRIPTION:
#module for evaluating column alerts



import numpy as np
import pandas as pd

def across_axis_alert(xz_tilt, xy_tilt, xz_vel, xy_vel, num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax):

    #DESCRIPTION
    #Evaluates node level alerts from node tilt and velocity data

    #INPUT
    #xz_tilt,xy_tilt, xz_vel, xy_vel:   Pandas DataFrame objects, with length equal to real-time window size, and columns for timestamp and individual node values
    #num_nodes:                         integer; number of nodes in a column
    #T_disp, TvelA1, TvelA2:            floats; threshold values for displacement, and velocities correspoding to alert levels A1 and A2
    #k_ac_ax:                           float; minimum proportion of minimum velocity to maximum velocity required to consider movement as valid

    #OUTPUT:
    #ac_ax:                             Pandas DataFrame object, with length equal to number of nodes, and columns for displacements along axes,
    #                                   displacement alerts, minimum and maximum velocities, velocity alerts and final node alerts

    print xz_tilt
    print xy_tilt
    print xz_vel
    print xy_vel

    #initializing DataFrame object, ac_ax
    ac_ax=pd.DataFrame(data=None, index=range(1,num_nodes+1))

    #evaluating net displacements within real-time window
    ac_ax['xz_disp']=np.round(xz_tilt.values[-1]-xz_tilt.values[13], 3)
    ac_ax['xy_disp']=np.round(xy_tilt.values[-1]-xy_tilt.values[13], 3)

    #checking if displacement threshold is exceeded in either axis
    cond = np.abs(ac_ax['xz_disp'].values)>T_disp, np.abs(ac_ax['xy_disp'].values)>T_disp
    ac_ax['disp_alert']=np.where(np.any(cond),

                                 #disp alert=1
                                 np.ones(len(ac_ax)),

                                 #disp alert=0
                                 np.zeros(len(ac_ax)))

    #getting minimum axis velocity value
    ac_ax['min_vel']=np.round(np.where(np.abs(xz_vel.values[-1])<np.abs(xy_vel.values[-1]),
                                       np.abs(xz_vel.values[-1]),
                                       np.abs(xy_vel.values[-1])), 4)

    #getting maximum axis velocity value
    ac_ax['max_vel']=np.round(np.where(np.abs(xz_vel.values[-1])>=np.abs(xy_vel.values[-1]),
                                       np.abs(xz_vel.values[-1]),
                                       np.abs(xy_vel.values[-1])), 4)
                                
    #checking if velocity threshold is exceeded
                        #checking if proportional velocity is present across node
    ac_ax['vel_alert']=np.where(ac_ax['min_vel'].values/ac_ax['max_vel'].values<k_ac_ax,   

                                #vel alert=0
                                np.zeros(len(ac_ax)),    

                                #checking if max node velocity exceeds threshold velocity for alert 1
                                np.where(ac_ax['max_vel'].values<=T_velA1,                  

                                         #vel alert=0
                                         np.zeros(len(ac_ax)),

                                         #checking if max node velocity exceeds threshold velocity for alert 2
                                         np.where(ac_ax['max_vel'].values<=T_velA2,         

                                                  #vel alert=1
                                                  np.ones(len(ac_ax)),

                                                  #vel alert=2
                                                  2*np.ones(len(ac_ax)))))

    
    ac_ax['node_alert']=np.where(ac_ax['vel_alert'].values==0,

                                 # node alert = displacement alert (0 or 1) if velocity alert is 0 
                                 ac_ax['disp_alert'].values,                                

                                 # node alert = velocity alert if displacement alert = 1 
                                 ac_ax['disp_alert'].values*ac_ax['vel_alert'].values)         
                                 
    print ac_ax
    
    return ac_ax

def adj_node_alert(ac_ax, adj_node_k,num_nodes_to_check):
    adj_node=[]
    for i in range(1,len(ac_ax)+1):
        if ac_ax['node_alert'][i-1].values>0:
            adj_node_ind=[]
            for s in range(1,num_nodes_to_check+1):
                if i-s>0: adj_node_ind.append(i-s)
                if i+s<=len(ac_ax):adj_node_ind.append(i+s)
            for j in adj_node_ind:
                if ac_ax['max_vel'][j-1].values>=ac_ax['max_vel'][i-1].values:
                    adj_node.append(ac_ax['node_alert'][i-1].values)
                    break
        else:
            adj_node.append(0)



