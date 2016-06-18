import os,time,re,sys
import MySQLdb
import datetime
import cfgfileio as cfg
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio
import senslopeServer as server
#---------------------------------------------------------------------------------------------------------------------------

def main():
    c = cfg.config()
    dbio.createTable("runtimelog","runtime")
    server.logRuntimeStatus("alert","checked")

    print '>> Checking for alert sms'
    alertmsg = server.CheckAlertMessages()
    if alertmsg:
        server.WriteOutboxMessageToDb(alertmsg,c.smsalert.smartnum)
        server.WriteOutboxMessageToDb(alertmsg,c.smsalert.globenum)
    else:
        print '>> No alert msg read.'
        
if __name__ == "__main__":
    main()
