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
import memcache
mc = memcache.Client(['127.0.0.1:11211'],debug=0)

if cfg.config().mode.script_mode == 'gsmserver':
    sys.path.insert(0, cfg.config().fileio.websocketdir)
    import dewsSocketLeanLib as dsll
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
        diff = (ts.minute%10) * 60 + ts.second
        ts = ts - td(seconds=diff)
        logtimestamp = ts.strftime("%Y-%m-%d %H:%M:00")
    else:
        logtimestamp = dt.today().strftime("%Y-%m-%d %H:%M:00")
    
    print ">> Logging runtime '" + status + "' at " + logtimestamp 
    
    query = """insert ignore into runtimelog
                (timestamp,script_name,status)
                values ('%s','%s','%s')
                """ %(logtimestamp,script_name,status)
    
    dbio.commitToDb(query, 'logRuntimeStatus', cfg.config().mode.logtoinstance)
       
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

def WriteRawSmsToDb(msglist,sensor_nums):
    query = "INSERT INTO smsinbox (timestamp,sim_num,sms_msg,read_status,web_flag) VALUES "
    for m in msglist:
        if sensor_nums.find(m.simnum[-10:]) == -1:
        # if re.search(m.simnum[-10:],sensor_nums):
            web_flag = 'W'
            print m.data[:20]
            if cfg.config().mode.script_mode == 'gsmserver':
                ret = dsll.sendReceivedGSMtoDEWS(str(m.dt.replace("/","-")), m.simnum, m.data)

                #if the SMS Message was sent successfully to the web socket server then,
                #   change web_flag to 'WS' which means "Websocket Server Sent"
                if ret == 0:
                    web_flag = 'WSS'
        else:
            web_flag = 'S'
        query += "('%s','%s','%s','UNREAD','%s')," % (str(m.dt.replace("/","-")),str(m.simnum),str(m.data.replace("'","\"")),web_flag)
        # query += "('" + str(m.dt.replace("/","-")) + "','" + str(m.simnum) + "','"
        # query += str(m.data.replace("'","\"")) + "','UNREAD'),"
    
    # just to remove the trailing ','
    query = query[:-1]
    # print query
    
    dbio.commitToDb(query, "WriteRawSmsToDb", instance='GSM')

def WriteEQAlertMessageToDb(alertmsg):
    c = cfg.config()
    WriteOutboxMessageToDb(alertmsg,c.smsalert.globenum)
    WriteOutboxMessageToDb(alertmsg,c.smsalert.smartnum)

def getGsmId(number):
    smart_prefixes = getAllowedPrefixes('SMART')
    globe_prefixes = getAllowedPrefixes('GLOBE')

    try:
        num_prefix = re.match("^((0)|(63))9\d\d",number).group()
    except:
        print '>> Unable to send sim number in this gsm module'
        return -1

    if num_prefix in smart_prefixes:
        return 'SMART'
    elif num_prefix in globe_prefixes:
        return 'GLOBE'
    else:
        print '>> Prefix', num_prefix, 'cannot be sent'
        return -1

def WriteOutboxMessageToDb(message,recepients,send_status='UNSENT'):
    for r in recepients.split(","):
        gsm_id = getGsmId(r)
        if gsm_id == -1:
            continue
        else:
            query = "INSERT INTO smsoutbox (timestamp_written,recepients,sms_msg,send_status,gsm_id) VALUES "
            tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
            query += "('%s','%s','%s','%s','%s')" % (tsw,r,message,send_status,gsm_id)
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

def getAllowedPrefixes(network):
    c = cfg.config()
    if network.upper() == 'SMART':
        prefix_list = c.simprefix.smart.split(',')
    else:
        prefix_list = c.simprefix.globe.split(',')

    extended_prefix_list = []
    for p in prefix_list:
        extended_prefix_list.append("639"+p)
        extended_prefix_list.append("09"+p)

    return extended_prefix_list
    
def SendMessagesFromDb(network,limit=10):
    c = cfg.config()
    if not c.mode.sendmsg:
        return
    allmsgs = dbio.getAllOutboxSmsFromDb("UNSENT",network,limit)
    if len(allmsgs) <= 0:
        # print ">> No messages in outbox"
        return

    print ">> Sending messagess from db"
        
    msglist = []
    for item in allmsgs:
        smsItem = gsmio.sms(item[0], str(item[2]), str(item[3]), str(item[1]))
        msglist.append(smsItem)
    allmsgs = msglist

    send_success_list = []
    fail_success_list = []

    allowed_prefixes = getAllowedPrefixes(network)
    # cycle through all messages
    for msg in allmsgs:
        # get recepient numbers in list
        recepient_list = msg.simnum.split(",")
        for num in recepient_list:
            try:
                num_prefix = re.match("^((0)|(63))9\d\d",num).group()
            except:
                continue
            # check if recepient number in allowed prefixed list    
            if num_prefix in allowed_prefixes:
                ret = gsmio.sendMsg(msg.data,num)
                if ret == 0:
                    send_success_list.append(msg.num)
                else:
                    fail_success_list.append(msg.num)
            else:
                print "Number not in prefix list", num_prefix

    dbio.setSendStatus("FAIL",fail_success_list)
    dbio.setSendStatus("SENT",send_success_list)

    #Get all outbox messages with send_status "SENT" and attempt to send
    #   chatterbox acknowledgements
    #   send_status will be changed to "SENT-WSS" if successful
    dsll.sendAllAckSentGSMtoDEWS()    
    
def getSensorNumbers():
    querys = "SELECT sim_num from site_column_sim_nums"

    # print querys

    nums = dbio.querydatabase(querys,'getSensorNumbers','LOCAL')

    return nums

def writeAlertToDb(alertmsg):
    dbio.createTable('smsalerts','smsalerts')

    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    query = "insert into smsalerts (ts_set,alertmsg,remarks) values ('%s','%s','none')" % (today,alertmsg)

    print query

    dbio.commitToDb(query,'writeAlertToDb','LOCAL')

def saveToCache(key,value):
    mc.set(key,value)

def getValueFromCache(key):
    value = mc.get(key)

def trySendingMessages(network):
    start = dt.now()
    while True:
        SendMessagesFromDb(network)
        print '.',
        time.sleep(2)
        if (dt.now()-start).seconds > 30:
            break

def deleteMessagesfromGSM():
    print "\n>> Deleting all read messages"
    try:
        gsmio.gsmcmd('AT+CMGD=0,2').strip()
        print 'OK'
    except ValueError:
        print '>> Error deleting messages'
        
def RunSenslopeServer(network):
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    lastAlertMsgSent = ''
    logruntimeflag = True
    global checkIfActive 
    checkIfActive = True

    try:
        gsm = gsmio.gsmInit(network)        
    except serial.SerialException:
        print '**NO COM PORT FOUND**'
        serverstate = 'serial'
        gsm.close()
        logRuntimeStatus(network,"com port error")
        raise ValueError(">> Error: no com port found")
            
    dbio.createTable("runtimelog","runtime",cfg.config().mode.logtoinstance)
    logRuntimeStatus(network,"startup")
    
    dbio.createTable('smsinbox','smsinbox',cfg.config().mode.logtoinstance)
    dbio.createTable('smsoutbox','smsoutbox',cfg.config().mode.logtoinstance)

    sensor_numbers_str = str(getSensorNumbers())

    print '**' + network + ' GSM server active**'
    print time.asctime()
    while True:
        m = gsmio.countmsg()
        if m>0:
            allmsgs = gsmio.getAllSms(network)
            
            try:
                WriteRawSmsToDb(allmsgs,sensor_numbers_str)
            except MySQLdb.ProgrammingError:
                print ">> Error: May be an empty line.. skipping message storing"
            
            deleteMessagesfromGSM()
                
            print dt.today().strftime("\n" + network + " Server active as of %A, %B %d, %Y, %X")
            logRuntimeStatus(network,"alive")

            trySendingMessages(network)
            
        elif m == 0:
            trySendingMessages(network)
            
            gsmio.gsmflush()
            today = dt.today()
            if (today.minute % 10 == 0):
                if checkIfActive:
                    print today.strftime("\nServer active as of %A, %B %d, %Y, %X")
                checkIfActive = False
            else:
                checkIfActive = True
                
        elif m == -1:
            print'GSM MODULE MAYBE INACTIVE'
            serverstate = 'inactive'
            logRuntimeStatus(network,"gsm inactive")
            gsmio.resetGsm()

        elif m == -2:
            print '>> Error in parsing mesages: No data returned by GSM'
            gsmio.resetGsm()            
        else:
            print '>> Error in parsing mesages: Error unknown'
            gsmio.resetGsm()
