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
    try:
        if sys.argv[1] == 'test': writetodb = False
        else: writetodb = True
    except:
        writetodb = True

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

        query = "INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg,send_status) VALUES "
        
        tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
        for item in contacts:
            message = 'SENSOR ALERT:\n%s' % (alertmsg)
            message = message.replace("ALERT","AL3RT")
            query += "('%s','%s','%s','UNSENT')," % (tsw,item[1],message)
        query = query[:-1]

        if writetodb: dbio.commitToDb(query, 'checkalertmsg', 'GSM')
        else: print query
        print 'done'
    else:
        print '>> No alert msg read.'
        
if __name__ == "__main__":
    main()
