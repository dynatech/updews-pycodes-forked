#import volatile.memory as mem
import re
import itertools
import MySQLdb, subprocess
from datetime import datetime as dt
from datetime import timedelta as td
import pandas as psql
import numpy as np
from dateutil.parser import parse
from sqlalchemy import create_engine
from time import localtime, strftime
import pandas as pd
import itertools
import smsclass
  
def lidar(sms):
    msg = sms.msg
    print(msg)
    values = {}
    matches = re.findall('(?<=\*)[A-Z]{2}\:[0-9\.]*(?=\*)', msg, re.IGNORECASE)
    MATCH_ITEMS = {
        "LR": {"name": "dist", "fxn": float},
        "BV": {"name": "voltage", "fxn": float},
        "BI": {"name": "current", "fxn": float},
        "TP": {"name": "temp_val", "fxn": float}
    }
    table_name = "lidar_data"
    conversion_count = 0

    for ma in matches:
        identifier, value = ma.split(":")

        if identifier not in MATCH_ITEMS.keys():
            print ("Unknown identifier", identifier)
            continue

        param = MATCH_ITEMS[identifier]

        try:
            values[param["name"]] = param["fxn"](value)
        except ValueError:
            print (">> Error: converting %s using %s" % (value, str(param["fxn"])))
            continue

        conversion_count += 1

    if conversion_count == 0:
        print (">> Error: no successful conversion")
        raise ValueError("No successful conversion of values")

    try:
        ts = re.search("(?<=\*)[0-9]{12}(?=$)",msg).group(0)
        ts = dt.strptime(ts,"%y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except AttributeError:
        raise ValueError("No valid timestamp recognized")

    values["ts"] = ts

    df_ext_values = pd.DataFrame([values])

    line = msg
    line = re.sub("(?<=\+) (?=\+)","NULL",line)
    linesplit = line.split('*')

    try:
        x = (linesplit[5])
        x = x.split(',')
        x = x[0]
        x = x.split(':')
        x = x[1] 
        
        y = (linesplit[5])
        y = y.split(',')
        y = y[1]

        z = (linesplit[5])
        z = z.split(',')
        z = z[2]
        
    except IndexError:
        raise ValueError("Incomplete data")


    df_ac = [{'ac_xval':x,'ac_yval':y,
            'ac_zval':z}]

    df_ac = pd.DataFrame(df_ac)

    line = re.sub("(?<=\+) (?=\+)","NULL",line)
    linesplit = line.split('*')

    try:
        x = (linesplit[6])
        x = x.split(',')
        x = x[0]
        x = x.split(':')
        x = x[1] 
        
        y = (linesplit[6])
        y = y.split(',')
        y = y[1]

        z = (linesplit[6])
        z = z.split(',')
        z = z[2]
        
    except IndexError:
        raise ValueError("Incomplete data")

#    txtdatetime = dt.strptime(linesplit[9],
#        '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

    df_mg = [{'mg_xval':x,'mg_yval':y,
            'mg_zval':z}]

    df_mg = pd.DataFrame(df_mg)

    line = re.sub("(?<=\+) (?=\+)","NULL",line)
    linesplit = line.split('*')

    try:
        x = (linesplit[7])
        x = x.split(',')
        x = x[0]
        x = x.split(':')
        x = x[1] 
        
        y = (linesplit[7])
        y = y.split(',')
        y = y[1]

        z = (linesplit[7])
        z = z.split(',')
        z = z[2]
        
    except IndexError:
        raise ValueError("Incomplete data")

    df_gr = [{'gr_xval':x,'gr_yval':y,
            'gr_zval':z}]

    df_gr = pd.DataFrame(df_gr)
    
    df_data = pd.concat([df_ext_values,df_ac,df_mg,df_gr], axis = 1)    

    return smsclass.DataTable(table_name,df_data)

## IMULA*L*LR:112.950*BV:8.45*BI:128.60*AC:9.5270,-0.1089,-0.3942*MG:0.0881,0.0755,-0.5267*GR:7.5512,9.0913,2.3975*TP:33.25*180807105005' ##