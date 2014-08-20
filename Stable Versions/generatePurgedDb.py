import os,time,re
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as tda
import pandas as pd
import numpy as np
from querySenslopeDb import *
import math
from timelyTrigger import *
import warnings
warnings.filterwarnings('ignore')

configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

section = "File I/O"

MFP = cfg.get(section,'MachineFilePath')
PurgedFP = MFP + cfg.get(section,'PurgedFilePath')
MonPurgedFP = MFP + cfg.get(section,'PurgedMonitoringFilePath')
LastGoodDataFP = MFP + cfg.get(section,'LastGoodDataFilePath')
NamedbPurged = cfg.get(DBIOSect,'NamedbPurged')

	
def WritePurgedFilesToDb():
    print 'Writing purged files to db'
    
    sites = GetSensorList()

    for site in sites:

        siteid = site.name
        print siteid, 

        CreateAccelTable(siteid, NamedbPurged)

        df = pd.read_csv("%s%s.csv" % (MonPurgedFP,siteid), names=['timestamp','id','xvalue','yvalue','zvalue','mvalue'], parse_dates=[0], index_col=0)
        df = df.where(pd.notnull(df), None)

        try:
            
            latestTs = GetLatestTimestamp('senslopedb_purged', siteid)
            # convert latest timestamp to timestamp
            #latestTs = dt.strptime(latestTs[0],"%y/%m/%d %H:%M:S")

            try:
                df = df[df.index > latestTs]
            except TypeError:
                print 'Empty database',

            db, cur = SenslopeDBConnect(NamedbPurged)
     
            count = 1
            len_line = 0

            if (len(df)==0):
                print 'Database updated.'
                continue
            
            split = int(math.ceil(math.ceil(len(df)/20000.0)/10)*10)

            for dfs in np.array_split(df, split):
                linep = '(%s/%s)' % (str(count),str(split))
                print linep,

                try:
                    dfs.to_sql(siteid, db, 'mysql', 'append')
                except AttributeError:
                    print dfs,

                count += 1
                if count>split:
                    break

                bspc = ''
                for i in range(0,len(linep)+2):
                    bspc = bspc + '\b'
                print bspc,

        except MySQLdb.OperationalError:
            print "(2006, 'MySQL server has gone away')"
        except MySQLdb.IntegrityError:
            print ".",
        db.close()
        
        print 'done'


def main():

    #while True:
    WritePurgedFilesToDb()

#        time.sleep(ReturnNextReportTime(30)+180)
    
if __name__ == '__main__':
    main()
  



