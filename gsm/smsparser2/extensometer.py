import volatile.memory as mem
import re
from datetime import datetime as dt
import pandas as pd
import smsclass

def uts(sms):
    values = {}

    uts_name = re.search("^[A-Z]{5}(?=\*U\*)",sms.msg).group(0)

    mc = mem.get_handle()
    
    DF_EXTENSOMETERS = mc.get("DF_EXTENSOMETERS")
    DATA_TABLE_NAME = "extensometer_uts_data"

    x_id = DF_EXTENSOMETERS[DF_EXTENSOMETERS["extensometer_name"] == uts_name.lower()]["extensometer_type"]
    x_id = int(x_id)

    if x_id == 0:
        return False
    else:
        values['extensometer_id'] = x_id   

    
    # uts_data = re.search("(?<=[A-Z]{5}\*U\*).*(?=\*[0-9]{12})", sms.msg).group(0).lower()

    matches = re.findall('(?<=\*)[A-Z]{2}\:[0-9\.]*(?=\*)', sms.msg, re.IGNORECASE)

    MATCH_ITEMS = {
        "LA": {"name": "lag", "fxn": int},
        "MX": {"name": "max_arg_val", "fxn": float},
        "MI": {"name": "max_arg_index", "fxn": int},
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
        return False

    ts = re.search("(?<=\*)[0-9]{12}(?=$)",sms.msg).group(0)
    ts = dt.strptime(ts,"%y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    values["ts"] = ts

    df_ext_values = pd.DataFrame([values])

    return smsclass.DataTable(DATA_TABLE_NAME, df_ext_values)

