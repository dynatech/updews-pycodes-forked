import ConfigParser
import re
from datetime import datetime as dt
from generatePurgedFiles import *
import time

# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

def ReturnNextReportTime(minute):
    # calculate sleep interval
    tm = dt.today()
    cur_sec = tm.minute*60 + tm.second
            
    sleep_tm = 0
    for i in range(0, 60*60+1, minute*60):
        if i > cur_sec:
            sleep_tm = i
            break
    print 'Sleep..',
    print sleep_tm - cur_sec
    return(sleep_tm - cur_sec)


def main():

    while True:
        print time.asctime()
        # place routines below
##        GenerateMonitoringPurgedFiles()
##        GenPurgedFiles()        
        
        time.sleep(ReturnNextReportTime(30))

    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Exception detected. Press anything to quit.'
        raw_input()

##    main()
##    raw_input()
