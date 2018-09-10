# -*- coding: utf-8 -*-
"""
Created on Thu Apr 07 09:29:47 2016

@author: SENSLOPEY
"""

import pandas as pd
import analysis.querydb as qDb
import volatile.memory as memory



v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']


def filter_outlier(df): 
#Checking of variables
        
    if (df.type_num[0] == 110 or df.type_num[0] == 10):
        mode = 0
    else:
        mode = 1
    

    soms_min=[[2000,500],[0,0]]                         #format: [[v2raw_min, v3raw_min], [v2calib_min,v3calib_min]]
    soms_max=[[7800,1600],[1700,1500]]

    if df.tsm_name[0].upper() in v2:
        ver = 0
    else:
        ver = 1
            
    df= df[(df.mval1>soms_min[mode][ver])&(df.mval1<soms_max[mode][ver])]
    try:   
        df = df.set_index('ts')
        df_outlier= df.resample('30Min',base=0).first()
    except:
        return df_outlier
    
    return df_outlier
    
def filter_undervoltage(df):
    '''for v3 only'''
    
    column = df.tsm_name[0]
    node = df.node_id[0]
#    seek_undervoltage(df,column,node,mode)
    df = df.set_index('ts')
    df = df.resample('30Min',base=0).first()
    df = df.drop(['data_id', 'node_id', 'type_num', 'tsm_name'], axis=1)
    
    volt_a1 = voltage_compute(column,node,1)
    volt_a2 = voltage_compute(column,node,2) 
    
    merge_voltage=pd.concat([df,volt_a1.v1,volt_a2.v2],axis=1,ignore_index=True)
    merge_voltage.columns=['mval1','v1','v2']
    df=merge_voltage.mval1[((merge_voltage.v1>3.2) & (merge_voltage.v1<3.4) & (merge_voltage.v2>3.2) & (merge_voltage.v2<3.4)) | 
            (merge_voltage.v1.isnull() & merge_voltage.v2.isnull())]
    df_undervoltage = df.reset_index()
    return df_undervoltage

def voltage_compute(column, node, a_num):
    
    tsm_details=memory.get("DF_TSM_SENSORS")
    #For blank tsm_name
    if not column:
        raise ValueError('enter valid column')
    if (a_num >= 3):
        raise ValueError('enter valid accelerometer number')
    
    #For invalid node    
    check_num_seg=tsm_details[tsm_details.tsm_name == column].reset_index().number_of_segments[0]

    if (int(node) > int(check_num_seg)):
        raise ValueError('Invalid node id. Exceeded number of nodes')
    
    df_voltage = qDb.get_raw_accel_data(tsm_name = column, 
                                        node_id = node, 
                                        accel_number = a_num, 
                                        batt=True, return_db=True)
    df_voltage.index = df_voltage.ts
    df_voltage.rename(columns={'batt':'v'+str(a_num)}, inplace= True)
    df_voltage=df_voltage.resample('30Min', base = 0).first()

    return df_voltage