# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 15:45:53 2020

@author: User
"""

import pandas as pd

from datetime import datetime as dt
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import smsclass


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
