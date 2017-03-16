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
import argparse
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
    # WriteOutboxMessageToDb(alertmsg,c.smsalert.globenum)
    # WriteOutboxMessageToDb(alertmsg,c.smsalert.smartnum)

def getGsmId(number):
    smart_prefixes = getAllowedPrefixes('SMART')
    globe_prefixes = getAllowedPrefixes('GLOBE')

    try:
        num_prefix = re.match("^((0)|(63))9\d\d",number).group()
    except:
        print '>> Unable to send sim number in this gsm module'
        return -1

    if num_prefix in smart_prefixes:
        return 3
        # return 'SMART'
    elif num_prefix in globe_prefixes:
        return 2
        # return 'GLOBE'
    else:
        print '>> Prefix', num_prefix, 'cannot be sent'
        return -1

def WriteOutboxMessageToDb(table='users',message='',recepients=''):
    if table == '':
        print "Error: No table indicated"
        return

    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    query = "insert into smsoutbox_%s (ts_written,sms_msg,source) VALUES ('%s','%s','central')" % (table,tsw,message)
    print query

    last_insert = dbio.commitToDb(query,'WriteOutboxMessageToDb',last_insert=True)[0][0]

    print 'Last insert:', last_insert

    logger_mobile = getLoggerContacts()

    query = "INSERT INTO smsoutbox_%s_status (outbox_id,mobile_id,gsm_id) VALUES " % (table[:-1])
            
    for r in recepients.split(","):
        gsm_id = getGsmId(r)
        if gsm_id == -1:
            continue
        else:
            tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
            print last_insert, logger_mobile[r], tsw, gsm_id
            query += "(%d,%d,%d)," % (last_insert,logger_mobile[r],gsm_id)
            # print query
    
    query = query[:-1]
    print query        
    dbio.commitToDb(query, "WriteOutboxMessageToDb")
    
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
    
def SendMessagesFromDb(table='users',send_status=0,gsm_id=0,limit=10):
    c = cfg.config()
    # if not c.mode.sendmsg:
    #     return
    allmsgs = dbio.getAllOutboxSmsFromDb(table,send_status,gsm_id,limit)
    if len(allmsgs) <= 0:
        # print ">> No messages in outbox"
        return
    
    print ">> Sending messagess from db"

    for m in allmsgs:
        print m
        
    # msglist = []
    # for item in allmsgs:
    #     smsItem = gsmio.sms(item[0], str(item[2]), str(item[3]), str(item[1]))
    #     msglist.append(smsItem)
    # allmsgs = msglist
    
    # success_list = []
    # fail_list = []

    # allowed_prefixes = getAllowedPrefixes(network)
    # # cycle through all messages
    # for msg in allmsgs:
    #     # get recepient numbers in list
    #     recepient_list = msg.simnum.split(",")
    #     # for num in recepient_list:
    #     try:
    #         num_prefix = re.match("^ *((0)|(63))9\d\d",num).group()
    #         num_prefix = num_prefix.strip()
    #     except:
    #         continue
    #         # check if recepient number in allowed prefixed list    
    #     if num_prefix in allowed_prefixes:
    #         ret = gsmio.sendMsg(msg.data,num.strip(),simulate=True)

    #         if ret:
    #             send_stat = 1
    #             send_status.append((msg.num,0))
    #         else:
    #             send_status.append(msg.num)
    #     else:
    #         print "Number not in prefix list", num_prefix

    # dbio.setSendStatus("FAIL",fail_success_list)
    # dbio.setSendStatus("SENT",send_success_list)

    #Get all outbox messages with send_status "SENT" and attempt to send
    #   chatterbox acknowledgements
    #   send_status will be changed to "SENT-WSS" if successful
    # dsll.sendAllAckSentGSMtoDEWS()    
    
def getSensorNumbers():
    querys = "SELECT sim_num from site_column_sim_nums"

    # print querys

    nums = dbio.querydatabase(querys,'getSensorNumbers','LOCAL')

    return nums

def getMobileSimNums(table):

    if table == 'loggers':
        query = """ 
        SELECT t1.mobile_id,t1.sim_num 
        FROM logger_mobile AS t1 
        LEFT OUTER JOIN logger_mobile AS t2 
            ON t1.sim_num = t2.sim_num 
                AND (t1.date_activated < t2.date_activated 
                OR (t1.date_activated = t2.date_activated AND t1.mobile_id < t2.mobile_id)) 
        WHERE t2.sim_num IS NULL and t1.sim_num is not null"""
    elif table == 'users':
        query = "select mobile_id,sim_num from user_mobile"
    else:
        print 'Error: table', table
        sys.exit()


    # print querys

    nums = dbio.querydatabase(query,'getSensorNumbers','sandbox')
    nums = {key: value for (value, key) in nums}

    return nums

def writeAlertToDb(alertmsg):
    dbio.createTable('smsalerts','smsalerts')

    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    query = "insert into smsalerts (ts_set,alertmsg,remarks) values ('%s','%s','none')" % (today,alertmsg)

    print query

    dbio.commitToDb(query,'writeAlertToDb')

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

def simulateGSM(network='simulate'):
    print "Simulating GSM"
    
    db, cur = dbio.SenslopeDBConnect('sandbox')
    
    smsinbox_sms = []

    try:
        query = """select sms_id, timestamp, sim_num, sms_msg from smsinbox
            where web_flag not in ('0','-1') limit 1000"""
    
        a = cur.execute(query)
        out = []
        if a:
            smsinbox_sms = cur.fetchall()
        
    except MySQLdb.OperationalError:
        print '9.',
        time.sleep(20)

    # print smsinbox_sms

    logger_mobile_sim_nums = getMobileSimNums('loggers')
    user_mobile_sim_nums = getMobileSimNums('users')
    # print logger_mobile

    # gsm_ids = getGsmIDs()
    # gsm_id = 1gsm_ids[network]
    gsm_id = 1
    loggers_count = 0
    users_count = 0
    
    query_loggers = "insert into smsinbox_loggers (ts_received,mobile_id,sms_msg,read_status,gsm_id) values "
    query_users = "insert into smsinbox_users (ts_received,mobile_id,sms_msg,read_status,gsm_id) values "

    print smsinbox_sms
    sms_id_ok = []
    sms_id_unk = []
    for m in smsinbox_sms:
        ts_received = m[1]
        sms_msg = m[3]
        read_status = 0  

        if m[2] in logger_mobile_sim_nums.keys():
            query_loggers += "('%s',%d,'%s',%d,%d)," % (ts_received,logger_mobile_sim_nums[m[2]],sms_msg,read_status,gsm_id)
            loggers_count += 1
        elif m[2] in user_mobile_sim_nums.keys():
            query_users += "('%s',%d,'%s',%d,%d)," % (ts_received,user_mobile_sim_nums[m[2]],sms_msg,read_status,gsm_id)
            users_count += 1
        else:
            print 'Unknown number', m[2]
            sms_id_unk.append(m[0])
            continue
        
        sms_id_ok.append(m[0])

    query_loggers = query_loggers[:-1]
    query_users = query_users[:-1]
    
    # print query
    
    if len(sms_id_ok)>0:
        if loggers_count > 0:
            dbio.commitToDb(query_loggers,'simulateGSM')
        if users_count > 0:
            dbio.commitToDb(query_users,'simulateGSM')
        
        sms_id_ok = str(sms_id_ok).replace("L","")[1:-1]
        query = "update smsinbox set web_flag = '0' where sms_id in (%s);" % (sms_id_ok)
        dbio.commitToDb(query,'simulateGSM')

    if len(sms_id_unk)>0:
        # print sms_id_unk
        sms_id_unk = str(sms_id_unk).replace("L","")[1:-1]
        query = "update smsinbox set web_flag = '-1' where sms_id in (%s);" % (sms_id_unk)
        dbio.commitToDb(query,'simulateGSM')
    
    sys.exit()
        
def RunSenslopeServer(network='simulate',table='loggers'):
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    lastAlertMsgSent = ''
    logruntimeflag = True
    global checkIfActive 
    checkIfActive = True

    if network == 'simulate':
        simulateGSM(network)
        sys.exit()

    try:
        gsm = gsmio.gsmInit(network)        
    except serial.SerialException:
        print '**NO COM PORT FOUND**'
        serverstate = 'serial'
        gsm.close()
        logRuntimeStatus(network,"com port error")
        raise ValueError(">> Error: no com port found")
            
    # dbio.createTable("runtimelog","runtime",cfg.config().mode.logtoinstance)
    # logRuntimeStatus(network,"startup",)
    
    # dbio.createTable('smsinbox','smsinbox',cfg.config().mode.logtoinstance)
    # dbio.createTable('smsoutbox','smsoutbox',cfg.config().mode.logtoinstance)

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

def getArguments():
    parser = argparse.ArgumentParser(description="Run SMS server [-options]")
    parser.add_argument("-t", "--table", help="smsinbox table (loggers or users)")
    parser.add_argument("-n", "--network", help="network name (smart/globe/simulate)")
    # parser.add_argument("-g", "--gsm", help="gsm name")
    # parser.add_argument("-s", "--status", help="inbox/outbox status",type=int)
    # parser.add_argument("-l", "--messagelimit", help="maximum number of messages to process at a time",type=int)
    # parser.add_argument("-r", "--runtest", help="run test function",action="store_true")
    # parser.add_argument("-b", "--bypasslock", help="bypass lock script function",action="store_true")
    
    try:
        args = parser.parse_args()

        # if args.status == None:
        #     args.status = 0
        # if args.messagelimit == None:
        #     args.messagelimit = 200
        return args        
    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()

def getGsmModules():
    ids = mc.get('gsmids')
    if ids == None:
        query = "select * from gsm_modules"
        gsm_modules = dbio.querydatabase(query,'getGsmIds')

        ids = dict() 
        for gsm_id,name,num in gsm_modules:
            ids[name] = gsm_id

        mc.set('gsm_ids',ids)

    return ids

def main():
    args = getArguments()
    network = args.network

    gsm_ids = getGsmModules()
    print gsm_ids.keys()

    if network not in gsm_ids.keys():
        print ">> Error in network selection", network
        sys.exit()
    
    RunSenslopeServer(network)
    sys.exit()

if __name__ == '__main__':
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print 'Bye'
            break
        except gsmio.CustomGSMResetException:
            print "> Resetting system because of GSM failure"
            gsmio.resetGsm()
            continue
