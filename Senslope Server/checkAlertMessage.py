import os,time,re,sys
import MySQLdb
import datetime
import cfgfileio as cfg
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio
import senslopeServer as server
import queryserverinfo
#---------------------------------------------------------------------------------------------------------------------------

def main():
    c = cfg.config()
    dbio.createTable("runtimelog","runtime")
    server.logRuntimeStatus("alert","checked")

    print '>> Checking for alert sms'
    alertmsg = server.CheckAlertMessages()

    print alertmsg
    if alertmsg:
        # server.WriteOutboxMessageToDb(alertmsg,c.smsalert.smartnum)
        # server.WriteOutboxMessageToDb(alertmsg,c.smsalert.globenum)
        query = """select nickname, numbers from dewslcontacts where grouptags like '%alert%'"""
        contacts = dbio.querydatabase(query,'checkalert')
        # print contacts

        query = "INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg) VALUES "
        timeofday = queryserverinfo.getTimeOfDayDescription()

        tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
        for item in contacts:
            message = 'SENSOR ALERT. Good %s %s\n%s' % (timeofday,item[0],alertmsg)
            query += "('%s','%s','%s')," % (tsw,item[1],message)
        query = query[:-1]

        dbio.commitToDb(query, 'checkalertmsg', 'GSM')
        print 'done'
    else:
        print '>> No alert msg read.'
        
if __name__ == "__main__":
    main()
