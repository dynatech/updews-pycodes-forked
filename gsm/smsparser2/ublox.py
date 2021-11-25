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

#"SINUA:16.723474503,120.781250000,2324.02,0.3946:33.00,4.04*211121150800"
def ublox_parser(sms):
    
    line = sms.msg
    split_line = line.split('*')
    
    
    data_line = split_line[0]
    ts = dt.strptime(split_line[1],'%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
    split_data = data_line.split(':')
    logger_name = split_data[0]

    ublox_data = split_data[1]
    logger_data = split_data[2]
    
    split_ublox_data = ublox_data.split(',')
    split_logger_data = logger_data.split(',')
    
    
    trans_ublox_data = pd.DataFrame(split_ublox_data).transpose()
    
    trans_ublox_data = trans_ublox_data.rename(columns={0: "latitude", 
                                            1: "longitude", 
                                            2: "msl",
                                            3: "prec"})
    
    trans_ublox_data["ts"] = ts
    
    trans_logger_data = pd.DataFrame(split_logger_data).transpose()
    trans_logger_data = trans_logger_data.rename(columns={0: "temp", 1: "volt"})    
    
    df = pd.concat([trans_ublox_data, trans_logger_data],axis =1)
    df = df[["ts","latitude","longitude", "msl", "prec","temp","volt"]]
    
    name_df = "ublox_{}".format(logger_name.lower())
    
    data = smsclass.DataTable(name_df,df)
    return data
