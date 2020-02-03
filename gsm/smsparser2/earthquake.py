import sys
import re
import pandas as pd
import numpy as np
import dynadb.db as dynadb
from datetime import datetime as dt
import smsclass

EQ_SMS_PATTERNS = {
    "date": re.compile(r"\d{1,2}\w+20[12][1234567890]", re.IGNORECASE),
    "time": re.compile(r"\d{1,2}[:\.]\d{1,2} *[AP]M", re.IGNORECASE),
    "magnitude": re.compile(r"((?<=M[SBLVOW]\=)|(?<=M\=)|"
        "(?<=MLV\=)|(?<=MWP\=))\d+\.\d+(?= )", re.IGNORECASE),
    "depth": re.compile(r"(?<=D)\D+(?<=[\=\:]) *\d+((?= )|(?=K*M))", re.IGNORECASE),
    "latitude": re.compile(r"\d+\.\d+(?=N)", re.IGNORECASE),
    "longitude": re.compile(r"\d+\.\d+(?=E)", re.IGNORECASE),
    "issuer": re.compile(r"(?<=\<)[A-Z\/]+(?=\>)", re.IGNORECASE)
}

def eq(sms):
    """
       - Process the sms message that fits for eq data.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **Dataframe**  - Return Dataframe structure output and if not return False for fail to parse message.

    """  
    pattern_matches = {}
    print(sms.msg)

    # search for matches
    for name in EQ_SMS_PATTERNS.keys():
        search_results = EQ_SMS_PATTERNS[name].search(sms.msg)
        if search_results:
            matched_pattern = search_results.group(0)
            if name == 'depth':
                int_pattern = re.compile(r"\d+", re.IGNORECASE)
                matched_pattern = int_pattern.search(matched_pattern).group(0)
            pattern_matches[name] = matched_pattern
        else:
            if name == 'issuer':
                pattern_matches['issuer'] = np.nan
            else:
                print ("No match for <%s> pattern." % (name),)
                print ("Incomplete message not stored.")
                return False

    print (pattern_matches)

    # format date
    datestr_init = pattern_matches["date"].upper()
    datestr = None
    try:
        datestr = pd.to_datetime(datestr_init).date()
    except:
        pass
    if datestr == None:
        print (">> Error in datetime conversion for <%s>" % (datestr_init))
        return False

    # format time
    timestr = pattern_matches["time"].replace(" ","").replace(".",":").upper()
    try:
        timestr = dt.strptime(timestr,"%I:%M%p").time()
    except:
        print (">> Error in datetime conversion", timestr)
        return False

    del pattern_matches["date"]
    del pattern_matches["time"]
    pattern_matches["ts"] = str(dt.combine(datestr, timestr))

    out = {}
    for col_name in pattern_matches.keys():
        out[col_name] = pattern_matches[col_name]

    df = pd.DataFrame([out])
    print (df)
    return smsclass.DataTable("earthquake_events", df)



