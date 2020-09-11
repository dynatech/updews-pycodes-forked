# -*- coding: utf-8 -*-

from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import re
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.smsparser2.smsclass as sms


def get_auth():
    """Authorize twitter access.
    """
    
    # Load credentials from json file
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "twitter_credentials.json"), "r") as file:
    		creds = json.load(file)
    # auth
    auth = OAuthHandler(creds['consumer_key'], creds['consumer_secret'])
    auth.set_access_token(creds['access_token'], creds['access_token_secret'])
    auth_api = API(auth)
    return auth_api


def get_eq_events(text):
    """Parse tweet text to get ts, magnitude, depth, latitude, longitude, 
    province.
    """

    # timestamp
    ts_re = r"(?<=Date and Time:)[ \:\-\w\d]*[AP]M"
    ts_fmt = "%d %b %Y - %I:%M %p"
    match = re.search(ts_re, text).group(0).strip()
    ts = str(datetime.strptime(match, ts_fmt))
    # magnitude
    mag_re = r"(?<=Magnitude =)[ \d\.]*"
    mag = str(re.search(mag_re, text).group(0).strip())
    # depth
    depth_re = r"(?<=Depth =)[ \d\.]*(?=kilometers)"
    depth = str(float(re.search(depth_re, text).group(0).strip()))
    # latitude and longitude
    coord_re = r"(?<=Location =)([ \d\.]*)N[ \,]*([ \d\.]*)(?=E)"
    match = re.search(coord_re, text)
    lat = str(float(match.group(1)))
    lon = str(float(match.group(2)))
    # province
    prov_re = r"(?<=Location =)([ \d\.\w\,\-\°]*)\(([ \w\)\(]*)(?=\))"
    prov = str(re.search(prov_re, text).group(2).split('(')[-1].strip())
    # consolidate into table
    df = pd.DataFrame({'ts': [ts], 'magnitude': [mag], 'depth': [depth],
                       'latitude': [lat], 'longitude': [lon],
                       'province': [prov], 'issuer': ['dyna']})
    return df


def main(hours=''):
    auth_api = get_auth()
    username = 'phivolcs_dost'
    
    if hours == '':
        try:
            hours = int(sys.argv[1])
        except:
            hours = 0.25
    end_date = datetime.utcnow() - timedelta(hours=hours)

    for status in Cursor(auth_api.user_timeline, id=username).items():
        text = auth_api.get_status(status.id, tweet_mode="extended").full_text
        if 'earthquake' in text.lower():
            try:
                print(status.created_at)
                df = get_eq_events(text)
                data_table = sms.DataTable('earthquake_events', df)
                db.df_write(data_table)
            except:
                pass
    
        if status.created_at < end_date:
            break

##################################################
if __name__ == '__main__':
    start = datetime.now()
    main()
    runtime = datetime.now() - start
    print('runtime =', runtime)
