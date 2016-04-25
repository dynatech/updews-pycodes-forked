import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import emailer
from senslopedbio import *
from gsmSerialio import *
from groundMeasurements import *
import multiprocessing
import SomsServerParser as SSP
import math
#---------------------------------------------------------------------------------------------------------------------------

def updateSimNumTable(name,sim_num,date_activated):
    db, cur = SenslopeDBConnect()
    
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

    commitToDb(query, 'updateSimNumTable')                
    
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
    
    commitToDb(query, 'logRuntimeStatus')
       
def timeToSendAlertMessage(ref_minute):
    current_minute = dt.now().minute
    if current_minute % AlertReportInterval != 0:
        return 0,ref_minute
    elif ref_minute == current_minute:
        return 0,ref_minute
    else:
        return 1,current_minute
    
def SendAlertEmail(network, serverstate):
    print "\n\n>> Attemptint to send routine emails.."
    sender = '1234dummymailer@gmail.com'
    sender_password = '1234dummy'
    receiver =['ggilbertluis@gmail.com', 'dynabeta@gmail.com']
	
    ## select if serial error if active server if inactive server
    if serverstate == 'active':
        subject = dt.today().strftime("ACTIVE " + network + " SERVER Notification as  of %A, %B %d, %Y, %X")
        active_message = '\nGood Day!\n\nYou received this email because ' + network + ' SERVER is still active!\nThanks!\n\n-' + network + ' Server\n'
    elif serverstate == 'serial':
        subject = dt.today().strftime(network + 'SERVER No Serial Notification  as  of %A, %B %d, %Y, %X')
        active_message = '\nGood Day!\n\nYou received this email because ' + network + ' SERVER is NOT connected to Serial Port!\nPlease fix me.\nThanks!\n\n-' + network + ' Server\n'
    elif serverstate == 'inactive':
        subject = dt.today().strftime(network + 'SERVER No Serial Notification  as  of %A, %B %d, %Y, %X')
        active_message = '\nGood Day!\n\nYou received this email because ' + network + ' SERVER is now INACTIVE!\\nPlease fix me.\nThanks!\n\n-' + network + ' Server\n'
	
    p = multiprocessing.Process(target=emailer.sendmessage, args=(sender,sender_password,receiver,sender,subject,active_message),name="sendingemail")
    p.start()
    time.sleep(60)
    # emailer.sendmessage(sender,sender_password,receiver,sender,subject,active_message)
    print ">> Sending email done.."
    
def SendAlertGsm(network,alertmsg):
    try:
        if network == 'GLOBE':    
            numlist = globenumbers.split(",")
        else:
            numlist = smartnumbers.split(",")
        # f = open(allalertsfile,'r')
        # alllines = f.read()
        # f.close()
        for n in numlist:
            sendMsg(alertmsg,n)
    except IndexError:
        print "Error sending all_alerts.txt"

def UnexpectedCharactersLog(msg, network):
    print ">> Error: Unexpected characters/s detected in ", msg.data
    f = open(unexpectedchardir+network+'Nonalphanumeric_errorlog.txt','a')
    f.write(msg.dt + ',' + msg.simnum + ',' + msg.data+ '\n')
    f.close()

def WriteRawSmsToDb(msglist):
    query = "INSERT INTO smsinbox (timestamp,sim_num,sms_msg,read_status) VALUES "
    
    for m in msglist:
        query += "('" + str(m.dt.replace("/","-")) + "','" + str(m.simnum) + "','" + str(m.data) + "','UNREAD'),"
    
    # just to remove the trailing ','
    query = query[:-1]
    
    commitToDb(query, "getAllSms")

def WriteOutboxMessageToDb(message,recepients,send_status='UNSENT'):
    query = "INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg,send_status) VALUES "
    
    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    query += "('%s','%s','%s','%s')" % (tsw,recepients,message,send_status)
    
    print query
    
    commitToDb(query, "WriteOutboxMessageToDb")
    
def CheckAlertMessages():
    alllines = ''
    if os.path.isfile(allalertsfile) and os.path.getsize(allalertsfile) > 0:
        f = open(allalertsfile,'r')
        alllines = f.read()
        f.close()
        
    return alllines
    
def SendMessagesFromDb(network):
    allmsgs = getAllOutboxSmsFromDb("UNSENT")
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
                sendMsg(msg.data,num)
                send_success_list.append(msg.num)
                
    setSendStatus("SENT",send_success_list)
        
def RunSenslopeServer(network):
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    email_flg = 0
    lastAlertMsgSent = ''
    logruntimeflag = True
    global checkIfActive

    try:
        gsmInit(network)        
    except serial.SerialException:
        print ">> ERROR: Could not open COM %r!" % (Port+1)
        print '**NO COM PORT FOUND**'
        serverstate = 'serial'
        # SendAlertEmail(network,serverstate)
        # while True:
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
        m = countmsg()
        if m>0:
            allmsgs = getAllSms(network)
            
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
                
            SendMessagesFromDb(network)
                
        elif m == -1:
            print'GSM MODULE MAYBE INACTIVE'
            serverstate = 'inactive'
            gsm.close()
            logRuntimeStatus(network,"gsm inactive")

        elif m == -2:
            print '>> Error in parsing mesages: No data returned by GSM'            
        else:
            print '>> Error in parsing mesages: Error unknown'
            
        # if os.path.isfile(allalertsfile) and os.path.getsize(allalertsfile) > 0:
            # f = open(allalertsfile,'r')
            # alllines = f.read()
            # f.close()
            # if lastAlertMsgSent != alllines:
                # print ">> Sending alert SMS"
                # lastAlertMsgSent = alllines
                # SendAlertGsm(network,alllines)
            # else:
                # print ">> Alert already sent"
            
""" Global variables"""
checkIfActive = True
anomalysave = ''

cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

# gsm = serial.Serial() 
Baudrate = cfg.getint('Serial', 'Baudrate')
Timeout = cfg.getint('Serial', 'Timeout')
Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')

AlertReportInterval = cfg.getint('SMSAlert','AlertReportInterval')
smsgndfile = cfg.get('SMSAlert','SMSgndmeasfile')
gndmeasfilesdir= cfg.get('SMSAlert','gndmeasfilesdir')

##SMS alert numbers
smartnumbers = cfg.get('SMSAlert', 'smartnumbers')
globenumbers = cfg.get('SMSAlert', 'globenumbers')

inboxdir = cfg.get('FileIO','inboxdir')
unknownsenderfile = cfg.get('FileIO','unknownsenderfile')
allalertsfile = cfg.get('FileIO','allalertsfile')

