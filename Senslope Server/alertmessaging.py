import os,time,re,sys
import MySQLdb
import datetime
import cfgfileio as cfg
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio
import senslopeServer as server
import queryserverinfo as qsi
#---------------------------------------------------------------------------------------------------------------------------

def checkAlertMessage():
    c = cfg.config()    
    dbio.createTable("runtimelog","runtime")
    server.logRuntimeStatus("alert","checked")

    alertfile = cfg.config().fileio.allalertsfile
    f = open(alertfile,'r')
    alertmsg = f.read()
    f.close()

    print alertmsg
    if not alertmsg:
        print '>> No alert msg read.'
        return

    # write alert message to db
    server.writeAlertToDb(alertmsg)

def getAlertStaffNumbers():
    query = """select nickname, numbers from dewslcontacts where grouptags like '%alert%'"""
    contacts = dbio.querydatabase(query,'checkalert')
    return contacts

def sendAlertMessage():
    # check due alert messages
    ts_due = dt.today()
    query = "select alert_id,alertmsg from smsalerts where ack = 'none' and ts_set <= '%s'" % (ts_due.strftime("%Y-%m-%d %H:%M:%S"))

    alertmsg = dbio.querydatabase(query,'SendAlertMessage')

    if alertmsg == None:
        print 'No alertmsg set for sending'
        return

    message = 'Alert ID %d:\n%s\n' % (alertmsg[0][0],alertmsg[0][1])
    message += 'Text ACK<space><alert id><space><remarks> to acknowledge.'

    contacts = getAlertStaffNumbers()

   
    for item in contacts:
        # for multile contacts
        for i in item[1].split(','):
            server.WriteOutboxMessageToDb(message,i)
    
    # set alert to 15 mins later
    ts_due = ts_due + td(seconds=60*15)
    query = "update smsalerts set ts_set = '%s' where alert_id = %s" % (ts_due.strftime("%Y-%m-%d %H:%M:%S"),alertmsg[0][0])

    dbio.commitToDb(query, 'checkalertmsg')


def processAckToAlert(msg):
    try:
        alert_id = re.search("(?<=K )\d+(?= )",msg.data).group(0)
    except:
        errmsg = "Error in parsing alert id. Please try again"
        server.WriteOutboxMessageToDb(errmsg,msg.simnum)
        return True

    try:
        name = qsi.getNameofStaff(msg.simnum)
    except:
        errmsg = "You are not permitted to acknowledge."
        server.WriteOutboxMessageToDb(errmsg,msg.simnum)
        return True

    try:
        remarks = re.search("(?<=\d ).+(?=$)",msg.data).group(0)
    except:
        errmsg = "Please put in your remarks."
        server.WriteOutboxMessageToDb(errmsg,msg.simnum)
        return True

    query = "update smsalerts set ack = '%s', ts_ack = '%s', remarks = '%s' where alert_id = %s" % (name,msg.dt,remarks,alert_id)
    dbio.commitToDb(query,processAckToAlert)

    contacts = getAlertStaffNumbers()
    message = "Alert ID %s ACK by %s on %s\nActions done: %s" % (alert_id,name,msg.dt,remarks)
    query = "INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg,send_status) VALUES "
    
    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    for item in contacts:
        message = message.replace("ALERT","AL3RT")
        query += "('%s','%s','%s','unsent')," % (tsw,item[1],message)
    query = query[:-1]

    dbio.commitToDb(query, 'checkalertmsg', 'GSM')

    return True

def main():
    try:
        if sys.argv[1] == 'test': 
            writetodb = False
        elif sys.argv[1] == 'checkalert':
            checkAlertMessage()
            sendAlertMessage()
        elif sys.argv[1] == 'sendalert':
            sendAlertMessage()
        else: 
            writetodb = True
    except IndexError:
        print 'Error: No arguments passed.'

if __name__ == "__main__":
    main()
