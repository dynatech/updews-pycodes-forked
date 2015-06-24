import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
from datetime import date, time
from sqlalchemy import create_engine
from querySenslopeDb import *

# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

DBIOSect = "DB I/O"
Hostdb = cfg.get(DBIOSect,'Hostdb')
Userdb = cfg.get(DBIOSect,'Userdb')
Passdb = cfg.get(DBIOSect,'Passdb')
Namedb = cfg.get(DBIOSect,'Namedb')
NamedbPurged = cfg.get(DBIOSect,'NamedbPurged')
printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')

def GenLsbAlerts():
    engine = create_engine('mysql+mysqldb://%s:%s@%s/%s' % (Userdb,Passdb,Hostdb,NamedbPurged))
    
    sites = GetSensorList()
    
    alertTxt = ""
    alertTxt2 = ""
    print "Getting lsb alerts"

    daydiff = 0
    #change "targetSite" to the site you wish to get the alerts from
    targetSite = "blcb"
    #change "d" to your target date
    #August - 30 days = July
    d = date(2014, 7, 31)
    t = time(0, 0)
    while (daydiff < 31):
        for site in sites:
            if (site.name != targetSite):
                continue	

            for nid in range(1, site.nos+1):
                #tcur = dt.now()-td(365)-td(daydiff)
                tcur = dt.combine(d, t)-td(daydiff)
                
                query = "select timestamp, xvalue, yvalue, zvalue from senslopedb_purged.%s where timestamp > '%s' and id=%s;" % (site.name, (tcur-td(7)).strftime("%y/%m/%d %H:%M:%S"), nid )
                #a = cur.execute(query)
                #.columns=['ts','id','x','y','z','m']
                df = pd.io.sql.read_sql(query,engine)
                df.columns = ['ts','x','y','z']
                df = df.set_index(['ts'])

                df2 = df.copy()
                dfa = []

                try:
                    df3 = df2.resample('30Min').fillna(method='pad')
                except pd.core.groupby.DataError:
                    #print "No data to resample %s %s" % (site.name, nid)
                    continue
                dfv = df3 - df3.shift(12) 

                if len(dfa) == 0:
                    dfa = dfv.copy()
                else:
                    dfa = dfa.append(dfv)

                window = 48
                dfarm = pd.rolling_mean(dfa, window)
                dfarm = dfarm[dfarm.index > dt.now()-td(1)]
                if (((abs(dfarm.x)>0.25) | (abs(dfarm.y)>0.25) | (abs(dfarm.z)>1.0)).any()):
                    ins = "%s,%s,%s" % (tcur.strftime("%y/%m/%d %H:%M:%S"), site.name, nid)
                    alertTxt += ins
                    alertTxt2 += ins
                    print ins + '\t',
                    
                    if ((abs(dfarm.x)>0.25).any()):
                        print 'x',
                        alertTxt += ',1'
                        alertTxt2 += ',' + repr(max(abs(dfarm.x)))
                    else:
                        alertTxt += ',0'
                        alertTxt2 += ',0'

                    if ((abs(dfarm.y)>0.25).any()):
                        print 'y',
                        alertTxt += ',1'
                        alertTxt2 += ',' + repr(max(abs(dfarm.y)))
                    else:
                        alertTxt += ',0'
                        alertTxt2 += ',0'
                    
                    if ((abs(dfarm.z)>1.0).any()):
                        print 'z',
                        alertTxt += ',1'
                        alertTxt2 += ',' + repr(max(abs(dfarm.z)))
                    else:
                        alertTxt += ',0'
                        alertTxt2 += ',0'

                    print ''
                    alertTxt += '\n'
                    alertTxt2 += '\n'

        daydiff = daydiff + 1
            
    f = open('lsb1month' + targetSite + '.csv', 'w')
    f.write(alertTxt2)
    f.close()

def main():
    
    GenLsbAlerts()
        
    
if __name__ == '__main__':
    main()
