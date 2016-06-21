import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio
import gsmSerialio as gsmio
import multiprocessing
import SomsServerParser as SSP
import math
import cfgfileio as cfg
#---------------------------------------------------------------------------------------------------------------------------

def updateSimNumTable(name,sim_num,date_activated):
    db, cur = dbio.SenslopeDBConnect('local')
    
    while True:
        try:
            query = """select sim_num from senslopedb.site_column_sim_nums
                where name = '%s' """ % (name)
        
            a = cur.execute(query)
            if a:
                out = cur.fetchall()
                if (sim_num == out[0][0]):
                    print ">> Number already in database", name, out[0][0]
                    return
                                    
                break
            else:
                print '>> Number not in database', sim_num
                return
                break
        except MySQLdb.OperationalError:
            print '1.',
            raise KeyboardInterrupt
    
    query = """INSERT INTO senslopedb.site_column_sim_nums
                (name,sim_num,date_activated)
                VALUES ('%s','%s','%s')""" %(name,sim_num,date_activated)

    dbio.commitToDb(query, 'updateSimNumTable')
    
def logRuntimeStatus(script_name,status):
    if (status == 'alive'):
        ts = dt.today()
        roundmintoten = int(math.floor(ts.minute / 10.0)) * 10
        logtimestamp = "%d-%02d-%02d %02d:%02d:00" % (ts.year,ts.month,ts.day,ts.hour,roundmintoten)
    else:
        logtimestamp = dt.today().strftime("%Y-%m-%d %H:%M:00")
    
    print ">> Logging runtime '" + status + "' at " + logtimestamp 
    
    query = """insert ignore into senslopedb.runtimelog
                (timestamp,script_name,status)
                values ('%s','%s','%s')
                """ %(logtimestamp,script_name,status)
    
    dbio.commitToDb(query, 'logRuntimeStatus')
       
def SendAlertGsm(network,alertmsg):
    c = cfg.config()
    try:
        if network == 'GLOBE':    
            numlist = c.simprefix.globe.split(",")
        else:
            numlist = c.simprefix.smart.split(",")
        # f = open(allalertsfile,'r')
        # alllines = f.read()
        # f.close()
        for n in numlist:
            gsmio.sendMsg(alertmsg,n)
    except IndexError:
        print "Error sending all_alerts.txt"

def WriteRawSmsToDb(msglist):
    query = "INSERT INTO smsinbox (timestamp,sim_num,sms_msg,read_status) VALUES "
    
    for m in msglist:
        query += "('" + str(m.dt.replace("/","-")) + "','" + str(m.simnum) + "','" + str(m.data) + "','UNREAD'),"
    
    # just to remove the trailing ','
    query = query[:-1]
    
    dbio.commitToDb(query, "WriteRawSmsToDb", instance='GSM')

def WriteEQAlertMessageToDb(alertmsg):
    c = cfg.config()
    WriteOutboxMessageToDb(alertmsg,c.smsalert.globenum)
    WriteOutboxMessageToDb(alertmsg,c.smsalert.smartnum)

def WriteOutboxMessageToDb(message,recepients,send_status='UNSENT'):
    query = "INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg,send_status) VALUES "
    
    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    query += "('%s','%s','%s','%s')" % (tsw,recepients,message,send_status)
    
    print query
    
    dbio.commitToDb(query, "WriteOutboxMessageToDb", 'gsm')
    
def CheckAlertMessages():
    c = cfg.config()
    alllines = ''
    print c.fileio.allalertsfile
    if os.path.isfile(c.fileio.allalertsfile) and os.path.getsize(c.fileio.allalertsfile) > 0:
        f = open(c.fileio.allalertsfile,'r')
        alllines = f.read()
        f.close()
    else:
        print '>> Error in reading file', alllines
    return alllines
    
def SendMessagesFromDb(network):
    allmsgs = dbio.getAllOutboxSmsFromDb("UNSENT")
    if len(allmsgs) <= 0:
        # print ">> No messages in outbox"
        return
        
    msglist = []
    for item in allmsgs:
        smsItem = sms(item[0], str(item[2]), str(item[3]), str(item[1]))
        msglist.append(smsItem)
    allmsgs = msglist
    
    if network.upper() == 'SMART':
        prefix_list = cfg.get('simprefix','smart').split(',')
    else:
        prefix_list = cfg.get('simprefix','globe').split(',')
    
    extended_prefix_list = []
    for p in prefix_list:
        extended_prefix_list.append("639"+p)
        extended_prefix_list.append("09"+p)        
    
    send_success_list = []
    # cycle through all messages
    for msg in allmsgs:
        # get recepient numbers in list
        recepient_list = msg.simnum.split(",")
        for num in recepient_list:
            try:
                num_prefix = re.match("^((0)|(63))9\d\d",num).group()
            except:
                print '>> Unable to send sim number in this gsm module'
                continue
            # check if recepient number in allowed prefixed list    
            if num_prefix in extended_prefix_list:
                gsmio.sendMsg(msg.data,num)
                send_success_list.append(msg.num)
                
    setSendStatus("SENT",send_success_list)
        
def RunSenslopeServer(network):
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    lastAlertMsgSent = ''
    logruntimeflag = True
    global checkIfActive

    try:
        gsmInit(network)        
    except serial.SerialException:
        print ">> ERROR: Could not open COM %r!" % (Port+1)
        print '**NO COM PORT FOUND**'
        serverstate = 'serial'
        gsm.close()
        logRuntimeStatus(network,"com port error")
        raise ValueError(">> Error: no com port found")
            
    createTable("runtimelog","runtime")
    logRuntimeStatus(network,"startup")
    
    createTable('smsinbox','smsinbox')
    createTable('smsoutbox','smsoutbox')

    print '**' + network + ' GSM server active**'
    print time.asctime()
    while True:
        m = gsmio.countmsg()
        if m>0:
            allmsgs = gsmio.getAllSms(network)
            
            try:
                WriteRawSmsToDb(allmsgs)
            except MySQLdb.ProgrammingError:
                print ">> Error: May be an empty line.. skipping message storing"
            
            # if not reading from database, uncomment the following line
            # ProcessAllMessages(allmsgs,network)
            
            # delete all read messages
            print "\n>> Deleting all read messages"
            try:
                gsmcmd('AT+CMGD=0,2').strip()
                print 'OK'
            except ValueError:
                print '>> Error deleting messages'
                
            print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
            logRuntimeStatus(network,"alive")
            time.sleep(10)
            
        elif m == 0:
            time.sleep(2)
            gsmflush()
            today = dt.today()
            if (today.minute % 10 == 0):
                if checkIfActive:
                    print today.strftime("\nServer active as of %A, %B %d, %Y, %X")
                checkIfActive = False
            else:
                checkIfActive = True
                
            # SendMessagesFromDb(network)
                
        elif m == -1:
            print'GSM MODULE MAYBE INACTIVE'
            serverstate = 'inactive'
            gsm.close()
            logRuntimeStatus(network,"gsm inactive")

        elif m == -2:
            print '>> Error in parsing mesages: No data returned by GSM'            
        else:
            print '>> Error in parsing mesages: Error unknown'
