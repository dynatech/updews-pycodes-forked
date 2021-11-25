# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 15:45:53 2020

@author: User
"""

import pandas as pd

from datetime import datetime as dt
import sys
import os
import re

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import smsclass

def twos_comp(val, bits):
    """compute the 2's complement of int value val"""
    if (val & (1 << (bits - 1))) != 0: # if sign bit is set e.g., 8bit: 128-255
        val = val - (1 << bits)        # compute negative value
    return val                         # return positive value as is

def stilt_parser(sms):
    
    line = sms.msg
    split_line = line.split('*')
    
    
    logger_name = split_line[0]
    indicator = split_line[1]
    data = split_line[2]
    
    
    data_split = data.split(',')
    
    trans_data = pd.DataFrame(data_split).transpose()
    
    trans_data = trans_data.rename(columns={0: "ac_x", 
                                            1: "ac_y", 
                                            2: "ac_z",
                                            3: "mg_x",
                                            4: "mg_y",
                                            5: "mg_z",
                                            6: "gr_x",
                                            7: "gr_y",
                                            8: "gr_z",
                                            9: "temp",
                                            10: "ts"})
    
    trans_data.ts[0] = dt.strptime(trans_data.ts[0],
                                   '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
    trans_data["type"]= indicator
    
    df = trans_data[["ts","type","ac_x", "ac_y", "ac_z","mg_x","mg_y", "mg_z","gr_x","gr_y","gr_z","temp"]]
    
    name_df = "stilt_{}".format(logger_name.lower())
    
    data = smsclass.DataTable(name_df,df)
    return data


"SINNB*0:C039,FAAE,000B,0A25,030B,085D,000C,0008,FFF6,F734,;1:C0BE,F99B,0031,00D6,088A,0574,0004,001B,FFEA,F4D5;0,18.75,4.14*211125161003"

def stilt_v2_parser(sms):
    line = sms.msg
    split_line = line.split('*')
    
    
    logger_name = split_line[0]
    data = split_line[1]
    ts = dt.strptime(split_line[2],'%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
    
    
    data_split = data.split(';')
    
    node0_data = data_split[0]
    node0_data_split = re.split(':|,',node0_data)
    
    for i in range(1,11):
        node0_data_split[i] = twos_comp(int(node0_data_split[i],16),16)
   
    df0 = pd.DataFrame(node0_data_split).transpose()
    df0 = df0.rename(columns={0:"node_id",1: "ac_x", 
                                    2: "ac_y", 
                                    3: "ac_z",
                                    4: "mg_x",
                                    5: "mg_y",
                                    6: "mg_z",
                                    7: "gr_x",
                                    8: "gr_y",
                                    9: "gr_z",
                                    10: "temp"})
    try:
        df0 = df0.drop(columns=[11])
    except:
        print("no column 11")
        
    try:
        node1_data = data_split[1]
        node1_data_split = re.split(':|,',node1_data)
        for i in range(1,11):
            node1_data_split[i] = twos_comp(int(node1_data_split[i],16),16)
   
        df1 = pd.DataFrame(node1_data_split).transpose()
        df1 = df1.rename(columns={0:"node_id",1: "ac_x", 
                                            2: "ac_y", 
                                            3: "ac_z",
                                            4: "mg_x",
                                            5: "mg_y",
                                            6: "mg_z",
                                            7: "gr_x",
                                            8: "gr_y",
                                            9: "gr_z",
                                            10: "temp"})
    except:
        print("No node 1")
        df1 = pd.DataFrame()
    logger_data = data_split[2]
    logger_data_split = logger_data.split(',')
    dflogger = pd.DataFrame(logger_data_split).transpose()
    dflogger = dflogger.rename(columns={0:"taps", 1: "temp_rtc", 2: "volt"})
    
    df_ts = pd.DataFrame({'ts':[ts]})
    df = pd.concat([df0, df1],axis =0)
    df = pd.concat([df_ts,df, dflogger],axis =1)
#    
#    df = trans_data[["ts","type","ac_x", "ac_y", "ac_z","mg_x","mg_y", "mg_z","gr_x","gr_y","gr_z","temp"]]

    name_df = "stilt_{}".format(logger_name.lower())
    
    data = smsclass.DataTable(name_df,df)
    return data
