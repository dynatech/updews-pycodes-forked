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
    ac_ax['xz_disp']=xz_tilt[-1].values-xz_tilt[0].values
    ac_ax['xy_disp']=xy_tilt[-1].values-xy_tilt[0].values

    #checking if displacement threshold is exceeded in either axis
    ac_ax['disp_alert']=np.where(np.abs(ac_ax['xz_disp'].values)>T_disp or
                                 np.abs(ac_ax['xy_disp'].values)>T_disp,

                                 #disp alert=1
                                 np.ones(len(ac_ax)),

                                 #disp alert=0
                                 np.zeros(len(ac_ax)))

    #getting minimum axis velocity value
    ac_ax['min_vel']=np.where(np.abs(xz_vel[-1].values)<np.abs(xy_vel[-1].values),
                              np.abs(xz_vel[-1].values),
                              np.abs(xy_vel[-1].values))

    #getting maximum axis velocity value
    ac_ax['max_vel']=np.where(np.abs(xz_vel[-1].values)>=np.abs(xy_vel[-1].values),
                              np.abs(xz_vel[-1].values),
                              np.abs(xy_vel[-1].values))
                                
    #checking if velocity threshold is exceeded
                        #checking if proportional velocity is present across node
    ac_ax['vel_alert']=np.where(ac_ax['min_vel'].vlues/ac_ax['max_vel'].values<k_ac_ax,   

                                #vel alert=0
                                np.zeros(len(ac_ax)),    

                                #checking if max node velocity exceeds threshold velocity for alert 1
                                np.where(ac_ax['max_va'].values<=TvelA1,                  

                                         #vel alert=0
                                         np.zeros(len(ac_ax)),

                                         #checking if max node velocity exceeds threshold velocity for alert 2
                                         np.where(ac_ax['max_va'].values<=TvelA2,         

                                                  #vel alert=1
                                                  np.ones(len(ax_Ax)),

                                                  #vel alert=2
                                                  2*np.ones(len(ax_Ax)))))

    
    ac_ax['node_alert']=np.where(ac_ax['vel_alert'].values==0,

                                 # node alert = displacement alert (0 or 1) if velocity alert is 0 
                                 ac_ax['disp_alert'].values,                                

                                 # node alert = velocity alert if displacement alert = 1 
                                 ac_ax['disp_alert'].values*ac_ax['vel_alert'].values)         
                                 
    print ac_ax
    
    return ac_ax





