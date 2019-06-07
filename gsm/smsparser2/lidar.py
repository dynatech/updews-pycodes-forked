import volatile.memory as mem
import re
from datetime import datetime as dt
import pandas as pd
import smsclass
from sqlalchemy import create_engine


def lidar(sms):
    values = {}
    matches = re.findall('(?<=\*)[A-Z]{2}\:[0-9\.]*(?=\*)', sms, re.IGNORECASE)

    MATCH_ITEMS = {
        "LR": {"name": "dist", "fxn": float},
        "BV": {"name": "voltage", "fxn": float},
        "BI": {"name": "current", "fxn": float},
        "TP": {"name": "temp_val", "fxn": float}
    }

    conversion_count = 0

    for ma in matches:
        identifier, value = tuple(ma.split(":"))

        if identifier not in MATCH_ITEMS.keys():
            print "Unknown identifier", identifier
            continue

        param = MATCH_ITEMS[identifier]

        try:
            values[param["name"]] = param["fxn"](value)
        except ValueError:
            print ">> Error: converting %s using %s" % (value, str(param["fxn"]))
            continue

        conversion_count += 1

    if conversion_count == 0:
        print ">> Error: no successful conversion"
        raise ValueError("No successful conversion of values")

    try:
        ts = re.search("(?<=\*)[0-9]{12}(?=$)",sms).group(0)
        ts = dt.strptime(ts,"%y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except AttributeError:
        raise ValueError("No valid timestamp recognized")

    values["ts"] = ts

    df_ext_values = pd.DataFrame([values])

    print df_ext_values

    return df_ext_values

def accel(sms):
    line = sms
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

    txtdatetime = dt.strptime(linesplit[9],
        '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

    df_data = [{'ts':txtdatetime,'xval':x,'yval':y,
            'zval':z}]

    df_data = pd.DataFrame(df_data)
    return df_data

def mg(sms):
    line = sms
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

    txtdatetime = dt.strptime(linesplit[9],
        '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

    df_data = [{'ts':txtdatetime,'xval':x,'yval':y,
            'zval':z}]

    df_data = pd.DataFrame(df_data)
    return df_data

def gr(sms):
    line = sms
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

    txtdatetime = dt.strptime(linesplit[9],
        '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

    df_data = [{'ts':txtdatetime,'xval':x,'yval':y,
            'zval':z}]

    df_data = pd.DataFrame(df_data)
    return df_data



## 'IMULA*L*LR:112.950*BV:8.45*BI:128.60*AC:9.5270,-0.1089,-0.3942*MG:0.0881,0.0755,-0.5267*GR:7.5512,9.0913,2.3975*TP:33.25*180807105005' ##