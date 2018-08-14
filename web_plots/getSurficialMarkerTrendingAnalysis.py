import sys
from analysis.surficial import markeralerts as ma
import json
import querydb as qdb

sc = qdb.memcached()

#site_id = 1
#marker_id = 197
#ts = "2018-05-09 00:00:00"

site_id = int(sys.argv[1])
marker_id = int(sys.argv[2])
ts = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")

def get_marker_trending_analysis_json(site_id, marker_id, ts):
    num_pts = sc['surficial']['surficial_num_pts']
    
    surficial_data_df = ma.get_surficial_data(site_id,ts,num_pts)
    marker_data_df = surficial_data_df[surficial_data_df.marker_id==marker_id]
    return_json = ma.evaluate_trending_filter(marker_data_df,to_plot=False,to_json=True)
    
    print "web_plots=" + json.dumps(return_json)

get_marker_trending_analysis_json(site_id, marker_id, ts)