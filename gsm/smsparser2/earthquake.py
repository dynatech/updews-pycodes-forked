import sys
import re
import pandas as pd
import numpy as np
import dynadb.db as dynadb
from datetime import datetime as dt
import smsclass

EQ_SMS_PATTERNS = {
    "date": re.compile(r"\d{1,2}\w+201[6789]", re.IGNORECASE),
    "time": re.compile(r"\d{1,2}[:\.]\d{1,2} *[AP]M", re.IGNORECASE),
    "magnitude": re.compile(r"((?<=M[SBLVOW]\=)|(?<=M\=)|"
        "(?<=MLV\=))\d+\.\d+(?= )", re.IGNORECASE),
    "depth": re.compile(r"(?<=D\=)\d+(?=K*M)", re.IGNORECASE),
    "latitude": re.compile(r"\d+\.\d+(?=N)", re.IGNORECASE),
    "longitude": re.compile(r"\d+\.\d+(?=E)", re.IGNORECASE),
    "issuer": re.compile(r"(?<=\<)[A-Z]+(?=\>)", re.IGNORECASE)
}

def eq(sms):

    pattern_matches = {}

    # search for matches
    for name in EQ_SMS_PATTERNS.keys():
        search_results = EQ_SMS_PATTERNS[name].search(sms.msg)
        if search_results:
            pattern_matches[name] = search_results.group(0)
        else:
            print "No match for <%s> pattern." % (name),
            print "Incomplete message not stored."
            return False

    print pattern_matches

    # format date
    datestr_init = pattern_matches["date"].upper()
    pattern = ["%d%B%Y","%d%b%Y"]
    datestr = None
    for p in pattern:
        try:
            datestr = dt.strptime(datestr_init, p).strftime("%Y-%m-%d")
            break
        except:
            pass
    if datestr == None:
        print ">> Error in datetime conversion for <%s>" % (datestr_init)
        return False

    # format time
    timestr = pattern_matches["time"].replace(" ","").replace(".",":").upper()
    try:
        timestr = dt.strptime(timestr,"%I:%M%p").strftime("%H:%M:00")
    except:
        print ">> Error in datetime conversion", timestr
        return False

    del pattern_matches["date"]
    del pattern_matches["time"]
    pattern_matches["ts"] = "%s %s" % (datestr, timestr)

    out = {}
    for col_name in pattern_matches.keys():
        out[col_name] = pattern_matches[col_name]

    df = pd.DataFrame([out]).set_index("ts")
    print df
    return smsclass.DataTable("earthquake_events", df)



