import sys
from analysis.subsurface import vcdgen as vcd
import pandas as pd
import json
    
def get_vcd_data_json(site_column, end_ts, start_ts):
    json = vcd.vcdgen(site_column, end_ts, start_ts) 
    return "web_plots=" + json

if __name__ == "__main__":
#    site_column = "agbta"
#    end_ts = "2017-11-11 06:00:00"
#    start_ts = "2017-11-08 06:00:00"
    
#    site_column = "magta"
#    end_ts = "2017-07-11 06:00:00"
#    start_ts = "2017-07-08 06:00:00"
    
#    site_column = "jorta"
#    end_ts = "2017-03-11 06:00:00"
#    start_ts = "2017-03-08 06:00:00"
    
    site_column = sys.argv[1]
    end_ts = sys.argv[2].replace("n",'').replace("T"," ").replace("%20"," ")
    start_ts = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")
    
    json_data = get_vcd_data_json(site_column, end_ts, start_ts)
    print json_data