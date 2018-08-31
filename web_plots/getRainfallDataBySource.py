import sys
import numpy as np
import pandas as pd
from analysis.rainfall import rainfallalert as ra

def get_rainfall_data_by_source_json(rain_gauge, start_date, end_date, offset):
    data = ra.get_resampled_data(rain_gauge, offset, start_date, 
                             end_date, check_nd=False, is_realtime=False)

    if len(data) == 0:
        data = pd.DataFrame(columns=['ts', 'rain']).set_index('ts')
    
    # 1-day cumulative rainfall
    rainfall2 = data.rolling(min_periods=1, window=48).sum()
    rainfall2 = np.round(rainfall2,4)
    
    # 3-day cumulative rainfall
    rainfall3 = data.rolling(min_periods=1, window=144).sum()
    rainfall3 = np.round(rainfall3,4)
    
    # instantaneous, 1-day, and 3-day cumulative rainfall in one dataframe
    data['twentyfour_hr_cumulative'] = rainfall2.rain
    data['seventytwo_hr_cumulative'] = rainfall3.rain
    data = data[(data.index >= start_date)]
    data = data[(data.index <= end_date)]
    
    return "web_plots=" + data.reset_index() \
              .to_json(orient = "records", date_format = "iso") \
              .replace("T", " ").replace("Z", "") \
              .replace(".000", "")

if __name__ == "__main__":
    
    rain_gauge = sys.argv[1]
    start_date = sys.argv[2].replace("n",'').replace("T"," ").replace("%20"," ")
    end_date = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")
    offset = sys.argv[4].replace("n",'').replace("T"," ").replace("%20"," ")
#    
#    start_date = pd.to_datetime(start_date)
#    end_date = pd.to_datetime(end_date)
#    offset = pd.to_datetime(offset)
    
#    rain_gauge = "rain_bakg"
#    offset = pd.to_datetime("2009-11-01 00:00")
#    start_date = pd.to_datetime("2009-11-04 00:00")
#    end_date = pd.to_datetime("2009-11-11 00:00")
    
    json_data = get_rainfall_data_by_source_json(rain_gauge, start_date, end_date, offset)
    print json_data