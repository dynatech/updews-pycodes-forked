import sys
from analysis.rainfall import rainfall as rf

def get_rainfall_sources_json(site_codes):
    gauges = rf.rainfall_gauges()
    gauges = gauges[gauges.site_code.isin(site_codes)]
    gauges = gauges[["gauge_name", "data_source", "threshold_value", "distance"]]
    
    return "web_plots=" + gauges.to_json(orient = "records")

if __name__ == "__main__":
    
    site_code = sys.argv[1]
    #site_code = "agb"
    site_codes = [site_code]
    
    json_data = get_rainfall_sources_json(site_codes)
    print json_data