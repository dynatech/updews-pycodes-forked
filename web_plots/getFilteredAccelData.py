import sys
import pandas as pd
import analysis.querydb as query
from analysis.subsurface import filterdata as fd
    
def get_filtered_accel_data_json(site_column, start_date, end_date, node_id, version):
    return_data = pd.DataFrame()
    
    for a_id in accel_id:
        raw_data = query.get_raw_accel_data(
                tsm_name = site_column, from_time = start_date,
                to_time = end_date, node_id = node_id, accel_number = a_id,
                batt=True)
        
        filtered_data = fd.apply_filters(raw_data)
        
        combined_data = pd.DataFrame({"data": [raw_data, filtered_data], "type": ["raw", "filtered"]})
        
        if len(accel_id) == 1:
            return_data["v1"] = [combined_data]
        else:
            return_data[a_id] = [combined_data]
        
    return "web_plots=" + return_data.to_json(orient = "records", date_format = "iso") \
                            .replace("T", " ").replace("Z", "") \
                            .replace(".000", "")

if __name__ == "__main__":
#    site_column = sys.argv[1]
#    start_date = sys.argv[2].replace("n",'').replace("T"," ").replace("%20"," ")
#    end_date = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")
#    node_id = int(sys.argv[4])
#    version = int(sys.argv[5])

    site_column = "agbta"
    start_date = "2017-11-04 00:00"
    end_date = "2017-11-11 00:00"
    node_id = 1
    version = 2

    #site_column = "labt"
    #start_date = "2018-07-21 00:00"
    #end_date = "2018-07-28 00:00"
    #node_id = 1
    #version = 1

    accel_id = [1]
    if version == 2:
        accel_id.append(2)
    
    json = get_filtered_accel_data_json(site_column, start_date, end_date, node_id, version)
    print json
