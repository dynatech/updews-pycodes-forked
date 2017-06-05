import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import serverdbio as dbio
import gsmio
import multiprocessing
import somsparser as ssp
import math
import cfgfileio as cfg
import memcache
import argparse
mc = memcache.Client(['127.0.0.1:11211'],debug=0)

if cfg.config().mode.script_mode == 'gsmserver':
    sys.path.insert(0, cfg.config().fileio.websocketdir)
    import dewsSocketLeanLib as dsll
#---------------------------------------------------------------------------------------------------------------------------

def log_runtime_status(script_name,status):
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
    
    dbio.commit_to_db(query, 'log_runtime_status')
       
def send_alert_gsm(network,alertmsg):
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
            gsmio.send_msg(alertmsg,n)
    except IndexError:
        print "Error sending all_alerts.txt"

def write_raw_sms_to_db(msglist,sensor_nums):
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
    
    dbio.commit_to_db(query, "write_raw_sms_to_db", instance='GSM')

def write_eq_alert_message_to_db(alertmsg):
    c = cfg.config()
    # write_outbox_message_to_db(alertmsg,c.smsalert.globenum)
    # write_outbox_message_to_db(alertmsg,c.smsalert.smartnum)

def get_gsm_id(number):
    smart_prefixes = get_allowed_prefixes('SMART')
    globe_prefixes = get_allowed_prefixes('GLOBE')

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

def write_outbox_message_to_db(message='',recepients='',table='users'):
    if table == '':
        print "Error: No table indicated"
        raise ValueError
        return

    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    query = "insert into smsoutbox_%s (ts_written,sms_msg,source) VALUES ('%s','%s','central')" % (table,tsw,message)
    
    last_insert = dbio.commit_to_db(query,'write_outbox_message_to_db',last_insert=True)[0][0]

    table_mobile = get_mobile_sim_nums(table)

    query = "INSERT INTO smsoutbox_%s_status (outbox_id,mobile_id,gsm_id) VALUES " % (table[:-1])
            
    for r in recepients.split(","):
        gsm_id = get_gsm_id(r)
        if gsm_id == -1:
            continue
        else:
            tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
            print last_insert, table_mobile[r], tsw, gsm_id
            query += "(%d,%d,%d)," % (last_insert,table_mobile[r],gsm_id)
            # print query
    
    query = query[:-1]
    print query        
    dbio.commit_to_db(query, "write_outbox_message_to_db")
    
def check_alert_messages():
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

def get_allowed_prefixes(network):
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
    
def send_messages_from_db(table='users',send_status=0,gsm_id=0,limit=10):
    c = cfg.config()
    # if not c.mode.send_msg:
    #     return
    allmsgs = dbio.get_all_outbox_sms_from_db(table,send_status,gsm_id,limit)
    if len(allmsgs) <= 0:
        print ">> No messages in outbox"
        return
    
    print ">> Sending messagess from db"

    for m in allmsgs:
        print m

    table_mobile = get_mobile_sim_nums(table)
    inv_table_mobile = {v: k for k, v in table_mobile.iteritems()}
    # print inv_table_mobile
        
    msglist = []
    for stat_id,mobile_id,sms_msg in allmsgs:
        smsItem = gsmio.sms(stat_id, inv_table_mobile[mobile_id], sms_msg, '')
        msglist.append(smsItem)
    allmsgs = msglist
    
    status_list = []
    
    allowed_prefixes = get_allowed_prefixes('globe')
    # # cycle through all messages
    for msg in allmsgs:
        try:
            num_prefix = re.match("^ *((0)|(63))9\d\d",msg.simnum).group()
            num_prefix = num_prefix.strip()
        except:
            print 'Error getting prefix', msg.simnum
            continue
            # check if recepient number in allowed prefixed list    
        if num_prefix in allowed_prefixes:
            ret = gsmio.send_msg(msg.data,msg.simnum.strip(),simulate=True)

            today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
            if ret:
                send_stat = 1
                stat = msg.num,1,today
            else:
                stat = msg.num,5,today

            status_list.append(stat)
            
        else:
            print "Number not in prefix list", num_prefix

    dbio.set_send_status(table,status_list)
    
    #Get all outbox messages with send_status "SENT" and attempt to send
    #   chatterbox acknowledgements
    #   send_status will be changed to "SENT-WSS" if successful
    # dsll.sendAllAckSentGSMtoDEWS()    
    
def get_sensor_numbers():
    querys = "SELECT sim_num from site_column_sim_nums"

    # print querys

    nums = dbio.query_database(querys,'get_sensor_numbers','LOCAL')

    return nums

def get_mobile_sim_nums(table):

    if table == 'loggers':

        logger_mobile_sim_nums = mc.get('logger_mobile_sim_nums')
        if logger_mobile_sim_nums:
            return logger_mobile_sim_nums

        query = """ 
        SELECT t1.mobile_id,t1.sim_num 
        FROM logger_mobile AS t1 
        LEFT OUTER JOIN logger_mobile AS t2 
            ON t1.sim_num = t2.sim_num 
                AND (t1.date_activated < t2.date_activated 
                OR (t1.date_activated = t2.date_activated AND t1.mobile_id < t2.mobile_id)) 
        WHERE t2.sim_num IS NULL and t1.sim_num is not null"""

        nums = dbio.query_database(query,'get_mobile_sim_nums','sandbox')
        nums = {key: value for (value, key) in nums}

        logger_mobile_sim_nums = nums
        mc.set("logger_mobile_sim_nums",logger_mobile_sim_nums)

    elif table == 'users':

        user_mobile_sim_nums = mc.get('user_mobile_sim_nums')
        if user_mobile_sim_nums:
            return user_mobile_sim_nums
        
        query = "select mobile_id,sim_num from user_mobile"

        nums = dbio.query_database(query,'get_mobile_sim_nums','sandbox')
        nums = {key: value for (value, key) in nums}

        user_mobile_sim_nums = nums
        mc.set("user_mobile_sim_nums",user_mobile_sim_nums)

    else:
        print 'Error: table', table
        sys.exit()

    return nums

def write_alert_to_db(alertmsg):
    dbio.create_table('smsalerts','smsalerts')

    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    query = "insert into smsalerts (ts_set,alertmsg,remarks) values ('%s','%s','none')" % (today,alertmsg)

    print query

    dbio.commit_to_db(query,'write_alert_to_db')

def save_to_cache(key,value):
    mc.set(key,value)

def get_value_from_cache(key):
    value = mc.get(key)

def try_sending_messages(network):
    start = dt.now()
    while True:
        send_messages_from_db(network)
        print '.',
        time.sleep(2)
        if (dt.now()-start).seconds > 30:
            break

def detele_messages_from_gsm():
    print "\n>> Deleting all read messages"
    try:
        gsmio.gsmcmd('AT+CMGD=0,2').strip()
        print 'OK'
    except ValueError:
        print '>> Error deleting messages'

def simulate_gsm(network='simulate'):
    print "Simulating GSM"
    
    db, cur = dbio.db_connect('sandbox')
    
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
    logger_mobile_sim_nums = get_mobile_sim_nums('loggers')
    user_mobile_sim_nums = get_mobile_sim_nums('users')
    # print logger_mobile

    # gsm_ids = get_gsm_ids()
    # gsm_id = 1gsm_ids[network]
    gsm_id = 1
    loggers_count = 0
    users_count = 0
    
    query_loggers = "insert into smsinbox_loggers (ts_received,mobile_id,sms_msg,read_status,gsm_id) values "
    query_users = "insert into smsinbox_users (ts_received,mobile_id,sms_msg,read_status,gsm_id) values "

    print smsinbox_sms
    sms_id_ok = []
    sms_id_unk = []
    ts_received = 0
    ltr_mobile_id= 0

    for m in smsinbox_sms:
        ts_received = m[1]
        sms_msg = m[3]
        read_status = 0 
    
        if m[2] in logger_mobile_sim_nums.keys():
            query_loggers += "('%s',%d,'%s',%d,%d)," % (ts_received,logger_mobile_sim_nums[m[2]],sms_msg,read_status,gsm_id)
            ltr_mobile_id= logger_mobile_sim_nums[m[2]]
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
    query_lastText = 'UPDATE last_text_received SET inbox_id = (select max(inbox_id) from smsinbox_loggers) , ts = "{}" where mobile_id= {}'.format(ts_received, ltr_mobile_id)
    # print query_lastText    
    if len(sms_id_ok)>0:
        if loggers_count > 0:
            # query_safe= 'SET SQL_SAFE_UPDATES=0'
            # dbio.commit_to_db(query_safe,'simulate_gsm')
            dbio.commit_to_db(query_loggers,'simulate_gsm')
            print query_lastText
            dbio.commit_to_db(query_lastText,'simulate_gsm')
        if users_count > 0:
            dbio.commit_to_db(query_users,'simulate_gsm')
        
        sms_id_ok = str(sms_id_ok).replace("L","")[1:-1]
        query = "update smsinbox set web_flag = '0' where sms_id in (%s);" % (sms_id_ok)
        dbio.commit_to_db(query,'simulate_gsm')

    if len(sms_id_unk)>0:
        # print sms_id_unk
        sms_id_unk = str(sms_id_unk).replace("L","")[1:-1]
        query = "update smsinbox set web_flag = '-1' where sms_id in (%s);" % (sms_id_unk)
        dbio.commit_to_db(query,'simulate_gsm')
    
    sys.exit()
        
def run_server(network='simulate',table='loggers'):
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    lastAlertMsgSent = ''
    logruntimeflag = True
    global checkIfActive 
    checkIfActive = True

    if network == 'simulate':
        simulate_gsm(network)
        sys.exit()

    try:
        gsm = gsmio.init_gsm(network)        
    except serial.SerialException:
        print '**NO COM PORT FOUND**'
        serverstate = 'serial'
        gsm.close()
        log_runtime_status(network,"com port error")
        raise ValueError(">> Error: no com port found")
            
    # dbio.create_table("runtimelog","runtime",cfg.config().mode.logtoinstance)
    # log_runtime_status(network,"startup",)
    
    # dbio.create_table('smsinbox','smsinbox',cfg.config().mode.logtoinstance)
    # dbio.create_table('smsoutbox','smsoutbox',cfg.config().mode.logtoinstance)

    sensor_numbers_str = str(get_sensor_numbers())

    print '**' + network + ' GSM server active**'
    print time.asctime()
    while True:
        m = gsmio.countmsg()
        if m>0:
            allmsgs = gsmio.get_all_sms(network)
            
            try:
                write_raw_sms_to_db(allmsgs,sensor_numbers_str)
            except MySQLdb.ProgrammingError:
                print ">> Error: May be an empty line.. skipping message storing"
            
            detele_messages_from_gsm()
                
            print dt.today().strftime("\n" + network + " Server active as of %A, %B %d, %Y, %X")
            log_runtime_status(network,"alive")

            try_sending_messages(network)
            
        elif m == 0:
            try_sending_messages(network)
            
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
            log_runtime_status(network,"gsm inactive")
            gsmio.resetGsm()

        elif m == -2:
            print '>> Error in parsing mesages: No data returned by GSM'
            gsmio.resetGsm()            
        else:
            print '>> Error in parsing mesages: Error unknown'
            gsmio.resetGsm()

def get_arguments():
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

def get_gsm_modules():
    ids = mc.get('gsmids')
    if ids == None:
        query = "select * from gsm_modules"
        gsm_modules = dbio.query_database(query,'get_gsm_ids')

        ids = dict() 
        for gsm_id,name,num in gsm_modules:
            ids[name] = gsm_id

        mc.set('gsm_ids',ids)

    return ids

def main():
    args = get_arguments()
    network = args.network

    gsm_ids = get_gsm_modules()
    print gsm_ids.keys()

    if network not in gsm_ids.keys():
        print ">> Error in network selection", network
        sys.exit()
    
    run_server(network)
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
