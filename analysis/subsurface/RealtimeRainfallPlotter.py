##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ion()

from datetime import datetime
import pandas as pd

import AllRainfall as rain
import querySenslopeDb as qdb

def main():
    while True:
        test_specific_time = raw_input('test specific time? (Y/N): ').lower()
        if test_specific_time == 'y' or test_specific_time == 'n':
            break
    
    if test_specific_time == 'y':
        while True:
            try:
                end = pd.to_datetime(raw_input('plot end timestamp (format: 2016-12-31 23:30): '))
                break
            except:
                print 'invalid datetime format'
                continue
    else:
        end = datetime.now()
            
    while True:
        try:
            site = qdb.GetSensorList(raw_input('sensor name: '))[0].name[0:3]
            break
        except:
            print 'sensor name is not in the list'
            continue

    rain.main(site=site, end=end, alert_eval=False, plot=True,
              monitoring_end=False, positive_trigger=False)

###############################################################################

if __name__ == "__main__":
    main()