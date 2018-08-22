import sys
from analysis.surficial import markeralerts as ma
import json
import querydb as qdb

def get_marker_trending_analysis_df(site_id, marker_id, ts):
    sc = qdb.memcached()
    num_pts = sc['surficial']['surficial_num_pts']
    
    surficial_data_df = ma.get_surficial_data(site_id,ts,num_pts)
    marker_data_df = surficial_data_df[surficial_data_df.marker_id==marker_id]
    return_df = ma.evaluate_trending_filter(marker_data_df,to_plot=False,to_json=True)
    
    return return_df

def get_marker_trending_analysis_json(site_id, marker_id, ts):
    return_df = get_marker_trending_analysis_df(site_id, marker_id, ts)
    return_json = "web_plots=" + json.dumps(return_df)
    
    return return_json

def main(site_id, marker_id, ts):
    return_json = get_marker_trending_analysis_json(site_id, marker_id, ts)
    print return_json

if __name__ == "__main__":
    site_id = 1
    marker_id = 73
    ts = "2018-05-09 00:00:00"
    
#    site_id = int(sys.argv[1])
#    marker_id = int(sys.argv[2])
#    ts = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")
    
    main(site_id, marker_id, ts)