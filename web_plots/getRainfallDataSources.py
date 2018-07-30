import sys
from analysis.rainfall import rainfall as rf

#site_code = sys.argv[1]
site_code = "agb"
site_codes = [site_code]

gauges = rf.rainfall_gauges()
gauges = gauges[gauges.site_code.isin(site_codes)]
gauges = gauges[["gauge_name", "data_source", "threshold_value"]]

print gauges.to_json(orient = "records")
