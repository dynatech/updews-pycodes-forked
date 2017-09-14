#twitter observer

from urllib import urlopen # module for HTML parsing
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup as BS
import urllib2
import numpy as np

import querySenslopeDb as q
import eq_alert_gen as eq
import configfileio as cfg

url = 'http://www.phivolcs.dost.gov.ph/html/update_SOEPD/EQLatest.html'

def open_account(username):
    url = urlopen( "https://twitter.com/" + username)
    page = BS( url,'lxml' )
    url.close()
    return page

def get_max_tweet_id(page):
    info = page.find('div', {'class':'stream-container'})
    tweet_id = info['data-max-position']
    return tweet_id
    
def get_tweet(tweet_id,page):
    tweet = page.find("div",{'data-tweet-id':tweet_id})
    return tweet

def get_tweet_time(tweet):
    timestamp = tweet.find('span', {'class':'js-short-timestamp'})
    timestamp = timestamp['data-time-ms']
    timestamp = pd.to_datetime(timestamp,unit='ms')
    timestamp = timestamp.tz_localize('UTC').tz_convert('Asia/Manila')
    timestamp = timestamp.replace(tzinfo=None)
    return timestamp  

def get_tweet_text(tweet):
    text = tweet.find('p',{'class':'TweetTextSize'}).get_text()
    return text

def load_website(url=url):
    url = urllib2.urlopen(url)
    content = url.read()
    soup = BS(content,"lxml")
    soup = soup.get_text()
    return soup

def cleantext(text):
    text = text.replace('\n',' ')
    text = text.replace(u'\xb0','')
    text = text.replace(u'\xa0','')
    text = text.replace(u'\xba','')
    text = text.replace(u'\t','')
    text = text.replace(u'\r','')
    return text

def getEQ(text):
    ts = re.search('\d{2}\W+[A-Za-z]+\W+\d{4}\W+[-]\W+\d{2}[:]\d{2}\W+[A-Za-z]{2}',text).group(0)
    lat = re.search('\d{2}[.]\d{2}',text).group(0)
    lon = re.search('\d{3}[.]\d{2}',text).group(0)
    mag = re.search('\W\d{1}[.]\d{1}\W',text).group(0)
    dep = re.search(' \d{3} ',text).group(0)
    rel = re.search('\d{3}\W+[km]+\W+\w\W+\d{2}\W+\w\W+\w+\W+[A-Za-z\W*]+\([A-Za-z\W*]+\)',text).group(0)
    
    return pd.to_datetime(ts),float(lat),float(lon),float(mag),int(dep),rel

def parse_rel(rel):
    dist = re.search('\d{3}\W*km',rel).group(0).replace(' km','')
    heading = re.search('\w\W*\d{2}\W+\w\W+',rel).group(0).replace(' ','')
    
    muni = re.search('of [A-Za-z\W*]+', rel).group(0)
    muni = re.sub('\([A-Za-z\W*]+\)',"",muni)
    muni = re.sub('of\W*',"",muni)
    muni = muni.strip()
    
    province = re.search('\([A-Za-z\W*]+\)',rel).group(0)
    province = re.sub('[()]', '', province)
    
    return int(dist),heading,muni,province

def parse_to_df(eq_array):
    e = eq_array
    columns = ['e_id','timestamp','mag','depth','lat','longi','dist','heading','municipality','province','issuer','critdist','processed']
    eqdf = pd.DataFrame(columns=columns)
    eqdf.loc[0] = 'NULL'
    eqdf['timestamp'] = e[0]
    eqdf['lat'] = e[1]
    eqdf['longi']= e[2]
    eqdf['mag'] = e[3]
    eqdf['depth'] = e[4]
    
    rel = parse_rel(eq_array[-1])
    eqdf['dist'] = rel[0]
    eqdf['heading'] = rel[1].upper()
    eqdf['municipality'] = rel[2].upper()
    eqdf['province'] = rel[3].upper()
    
    eqdf['issuer'] = 'Twitter'
    eqdf['processed'] = 0
    
    return eqdf
    
def insert_to_db(eqdf):
    eqdf = eqdf.iloc[0]
    db_cols = 'timestamp, mag, depth, lat, longi, dist, heading, municipality, province, issuer, critdist, processed'

    query = "INSERT INTO %s.earthquake (%s) \n" % ('senslopedb',db_cols)
    query += "\t SELECT '%s', %s, %s, %s, %s, %s, '%s', '%s', '%s', '%s', %s, %s \n" % (eqdf.timestamp,eqdf.mag,eqdf.depth,eqdf.lat,eqdf.longi,eqdf.dist,eqdf.heading,eqdf.municipality,eqdf.province,eqdf.issuer,eqdf.critdist,eqdf.processed)
    query += "\t\t FROM dual \n"
    query += "\t\t WHERE NOT EXISTS (SELECT 1 FROM %s.earthquake \n" % ('senslopedb')
    query += "\t\t\t WHERE timestamp = '%s' \n" % (eqdf.timestamp)
    query += "\t\t\t AND mag = %s \n" % (eqdf.mag)
    query += "\t\t\t AND lat = %s \n" % (eqdf.lat)
    query += "\t\t\t AND longi = %s)" % (eqdf.longi)
    
    execQuery(query)
    return 0

#    print query
 
def execQuery(query):
        db, cur = q.SenslopeDBConnect(q.Namedb)
        cur.execute(query)
        db.commit()
        db.close()
    
    
    

'''
regex breakdown
timestamp:
    \d{2} 
    \W+ = one or more whitespace
    [A-Za-z]+ = month name
    \W+
    \d{4} = year
    \W+
    [-] = dash
    \W+
    \d{2} = hour
    [:] = colon
    \d{2} = minute
    \W+
    [A-Za-z]{2} = AM or PM

relative location:
    \d{3}\W+[km]+ = distance (e.g. 123 km)
    \W+
    \w\W+\d{2}\W+\w\W+\w+ = bearing (e.g. N 42 W of)
    \W+
    [A-Za-z\W*]+ = municipality name. 
    \(\[A-Za-z\W*]+\) = province name in parenthesis.
'''   

##########################################
username = 'phivolcs_dost'
url = 'http://www.phivolcs.dost.gov.ph/html/update_SOEPD/EQLatest.html'
keyword = "#EarthquakePH"
time_interval = 300
##########################################


page = open_account(username)
max_id = get_max_tweet_id(page)
tweet = get_tweet(max_id,page)
text = get_tweet_text(tweet)
time = get_tweet_time(tweet)

delta = datetime.now() - time
delta = ( delta.total_seconds()/60 )



if (keyword in text and delta < time_interval):
    text = load_website()
    text = cleantext(text)
    eq_array = getEQ(text)
    eqdf = parse_to_df(eq_array)
    insert_to_db(eqdf)
    eq.main()
    
else:
    print 'no new EQ'

    
    