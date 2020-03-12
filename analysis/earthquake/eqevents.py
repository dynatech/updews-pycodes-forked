### IMPORTANT matplotlib declarations must always be FIRST 
### to make sure that matplotlib works with cron-based automation
import platform
if platform.system() == 'Linux':
    import matplotlib as mpl
    mpl.use('Agg')
    import matplotlib.pyplot as plt
    plt.ioff()

from twitterscraper import query_tweets
from datetime import datetime, timedelta

import os
import pandas as pd
import re
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.smsparser2.smsclass as sms


def get_eq_events(ts=datetime.now()):
    tweets = query_tweets("#EarthquakePH", begindate=(ts-timedelta(1)).date(),
                          enddate=(ts+timedelta(1)).date())
    
    df = pd.DataFrame()
    
    for tweet in tweets:
        if tweet.screen_name == "phivolcs_dost":
            try:
                text = tweet.text
        
                # timestamp
                ts_re = r"(?<=Date and Time:)[ \:\-\w\d]*[AP]M"
                ts_fmt = "%d %b %Y - %I:%M %p"
                match = re.search(ts_re, text).group(0).strip()
                ts = pd.to_datetime(datetime.strptime(match, ts_fmt))
        
                # magnitude
                mag_re = r"(?<=Magnitude =)[ \d\.]*"
                mag = re.search(mag_re, text).group(0).strip()
        
                # depth
                depth_re = r"(?<=Depth =)[ \d\.]*(?=kilometers)"
                depth = float(re.search(depth_re, text).group(0).strip())
        
                # latitude and longitude
                coord_re = r"(?<=Location =)([ \d\.]*)N[ \,]*([ \d\.]*)(?=E)"
                match = re.search(coord_re, text)
                lat = float(match.group(1))
                lon = float(match.group(2))
                
                # province
                prov_re = r"(?<=Location =)([ \d\.\w\,\-\°]*)\(([ \w\)\(]*)(?=\))"
                prov = re.search(prov_re, text).group(2).split('(')[-1].strip()
                
                df = df.append(pd.DataFrame({'ts': [ts], 'magnitude': [mag],
                                             'depth': [depth],
                                             'latitude': [lat],
                                             'longitude': [lon],
                                             'province': [prov],
                                             'issuer': ['dyna']}),
    ignore_index=True)
            except:
                pass
            
    data_table = sms.DataTable('earthquake_events', df)
    db.df_write(data_table)
    
###############################################################################
if __name__ == "__main__":
    get_eq_events()
