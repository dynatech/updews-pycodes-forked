import os,time,serial,re,sys,traceback
import MySQLdb, subprocess
from datetime import datetime as dt
from datetime import timedelta as td
import somsparser as ssp
import argparse
import lockscript as lock
import alertmessaging as amsg
import memcache
import lockscript
import surficialparser as surfp
import utsparser as uts
import dynadb.db as dynadb
import smstables
import volatile.memory as mem
import smsparser2.subsurface as subsurface
import smsparser2 as parser
import smsparser2.smsclass as smsclass
import smsparser2.rain as rain

def logger_response(sms,log_type,log='False'):
    """
       - Log the id of the match expression on table logger_respose.
      
      :param sms: list data info of sms message .
      :param Log_type: list data info of sms message .
      :param Log: Switch on or off the logging of the response.
      :type sms: list
      :type sms: str
      :type sms: str, Default(False)
      :returns: N/A.

    """ 
    if log:
        query = ("INSERT INTO logger_response (`logger_Id`, `inbox_id`, `log_type`)"
         "values((Select logger_id from logger_mobile where sim_num = %s order by"
          " date_activated desc limit 1),'%s','%s')" 
         % (sms.sim_num,sms.inbox_id,log_type))
                    
        dynadb.write(query, 'insert new log for logger response',instance='sandbox')
        print '>> Log response'
    else:
        return False


def common_logger_sms(sms):
    """
       - Check sms message if matches to the regular expression.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **value** - Return the id value number of the match regular expression and Return False if not.

    """ 
    log_match = {
        'NO DATA FROM SENSELOPE':1,
        'PARSED':2,
        '^\w{4,5}\*0\*\*[0-9]{10,12}':2,
        '^ \*':3,
        '^\*[0-9]{10,12}$':3,
        '^[A-F0-9]+\*[0-9]{10,12}$':3,
        '^[A-F0-9]+\*[A-F0-9]{10,13}':3,
        '^[A-F0-9]+\*[A-F0-9]{6,7}':3,
        'REGISTERED':4,
        'SERVER NUMBER':5,
        '^MANUAL RESET':6,
        'POWER UP':7, 
        'SYSTEM STARTUP': 8,
        'SMS RESET':9, 
        'POWER SAVING DEACTIVATED':10,
        'POWER SAVING ACTIVATED':11,
        'NODATAFROMSENSLOPE':12,
        '^\w{4,5}\*[xyabcXYABC]\*[A-F0-9]+$':13,
        '!\*':15
    }
    for key,value in log_match.items():    
        if re.search(key, sms.msg.upper()):
            logger_response(sms,value,True)
            return value
    return False

def update_last_msg_received_table(txtdatetime,name,sim_num,msg):
    """
       - Update recieved message from the last_msg_recived table.
      
      :param txtdatetime: list data info of sms message .
      :param name: list data info of sms message .
      :param sim_num: list data info of sms message .
      :param msg: list data info of sms message .
      :type txtdatetime: date
      :type name: str
      :type sim_num: str
      :type msg: str    
      :returns: N/A.

    """
    query = ("insert into senslopedb.last_msg_received"
        "(timestamp,name,sim_num,last_msg) values ('%s','%s','%s','%s')"
        "on DUPLICATE key update timestamp = '%s', sim_num = '%s',"
        "last_msg = '%s'" % (txtdatetime, name, sim_num, msg, txtdatetime,
        sim_num,msg)
        )
                
    dynadb.write(query, 'update_last_msg_received_table')
    

def process_piezometer(sms):
    """
       - Process the sms message that fits for process_piezometer and save paserse message to database.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **has_parse_error**  - Return False for fail to parse message .

    """     
    #msg = message
    line = sms.msg
    sender = sms.sim_num
    print 'Piezometer data: ' + line
    line = re.sub("\*\*","*",line)
    try:
    #PUGBPZ*13173214*1511091800 
        linesplit = line.split('*')
        msgname = linesplit[0].lower()
        msgname = re.sub("due","",msgname)
        msgname = re.sub("pz","",msgname)
        msgname = re.sub("ff","",msgname)

        if len(msgname) == 3:
            msgname = msgname + 'pz'
        
        print 'msg_name: ' + msgname
        data = linesplit[1]
        data = re.sub("F","",data)        
        
        print "data:", data

        # msgid = int(('0x'+data[:4]), 16)
        # p1 = int(('0x'+data[4:6]), 16)*100
        # p2 = int(('0x'+data[6:8]), 16)
        # p3 = int(('0x'+data[8:10]), 16)*.01
        p1 = int(('0x'+data[:2]), 16)*100
        p2 = int(('0x'+data[2:4]), 16)
        p3 = int(('0x'+data[4:6]), 16)*.01
        piezodata = p1+p2+p3
        
        t1 = int(('0x'+data[6:8]), 16)
        t2 = int(('0x'+data[8:10]), 16)*.01
        tempdata = t1+t2
        try:
            txtdatetime = dt.strptime(linesplit[2],
                '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
        except ValueError:
            txtdatetime = dt.strptime(linesplit[2],
                '%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')

        if int(txtdatetime[0:4]) < 2009:
            txtdatetime = sms.ts
            
    except IndexError, AttributeError:
        print '\n>> Error: Piezometer message format is not recognized'
        print line
        return
    except ValueError:    
        print '>> Error: Possible conversion mismatch ' + line
        return      

        # try:
    # dbio.create_table(str(msgname), "piezo")
    try:
      query = ("INSERT INTO piezo_%s (ts, frequency_shift, temperature ) VALUES"
      " ('%s', %s, %s)") % (msgname,txtdatetime,str(piezodata), str(tempdata))
      # print query
        # print query
    except ValueError:
        print '>> Error writing query string.', 
        return False
   
    
    try:
        dynadb.write(query, 'process_piezometer')
    except MySQLdb.ProgrammingError:
        print '>> Unexpected programing error'
        return False
        
    print 'End of Process Piezometer data'
    return True

def check_logger_model(logger_name):
    query = ("SELECT model_id FROM senslopedb.loggers where "
        "logger_name = '%s'") % logger_name

    return dynadb.read(query,'check_logger_model')[0][0]
    
def spawn_alert_gen(tsm_name, timestamp):
    """
       - Process of sending data to alert generator for loggers and users.
      
      :param tsm_name: name of logger or user .
      :param timestamp: Data timestamp of message .
      :type tsm_name: str
      :type timestamp: date
      :returns: N/A.

    """
    # spawn alert alert_gens

    args = get_arguments()

    if args.nospawn:
        print ">> Not spawning alert gen"
        return

    print "For alertgen.py", tsm_name, timestamp
    # print timestamp
    timestamp = (dt.strptime(timestamp,'%Y-%m-%d %H:%M:%S')+\
        td(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
    # print timestamp
    # return

    mc = mem.get_handle()
    alertgenlist = mc.get('alertgenlist')

    if alertgenlist == None:
        mc.set('alertgenlist',[])
        print "Setting alertgenlist for the first time"
        alertgenlist = []

    alert_info = dict()

    # check if tsm_name is already in the list for processing
    for_processing = False
    for ai in alertgenlist:
        if ai['tsm_name'] == tsm_name.lower():
            for_processing = True
            break

    if for_processing:
        print tsm_name, "already in alert gen list"
    else:
        # insert tsm_name to list
        print "Adding", tsm_name, "to alert gen list"
        alert_info['tsm_name'] = tsm_name.lower()
        alert_info['ts'] = timestamp
        alertgenlist.insert(0, alert_info)
        mc.set('alertgenlist',[])
        mc.set('alertgenlist',alertgenlist)

def process_surficial_observation(sms):
    """
       - Process the sms message that fits for surficial observation and save paserse message to database.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **has_parse_error**  - Return False for fail to parse message .

    """
    sc = mem.server_config()
    has_parse_error = False
    
    obv = []
    try:
        obv = surfp.parse_surficial_text(sms.msg)
        print 'Updating observations'
        mo_id = surfp.update_surficial_observations(obv)
        surfp.update_surficial_data(obv,mo_id)
        # server.write_outbox_message_to_db("READ-SUCCESS: \n" + sms.msg,
        #     c.smsalert.communitynum,'users')
        # server.write_outbox_message_to_db(c.reply.successen, msg.simnum,'users')
        # proceed_with_analysis = True
    except surfp.SurficialParserError as e:
        print "stre(e)", str(e)
        errortype = re.search("(WEATHER|DATE|TIME|GROUND MEASUREMENTS|NAME|CODE)", 
            str(e).upper()).group(0)
        print ">> Error in manual ground measurement SMS", errortype
        has_parse_error = True

        # server.write_outbox_message_to_db("READ-FAIL: (%s)\n%s" % 
            # (errortype,sms.msg),c.smsalert.communitynum,'users')
        # server.write_outbox_message_to_db(str(e), msg.simnum,'users')
    except KeyError:
        print '>> Error: Possible site code error'
        # server.write_outbox_message_to_db("READ-FAIL: (site code)\n%s" % 
        #     (sms.msg),c.smsalert.communitynum,'users')
        has_parse_error = True
    # except:
    #     # pass
    #     server.write_outbox_message_to_db("READ-FAIL: (Unhandled) \n" + 
    #         sms.msg,c.smsalert.communitynum,'users')

    # spawn surficial measurement analysis
    proceed_with_analysis = sc['subsurface']['enable_analysis']
    # proceed_with_analysis = False
    if proceed_with_analysis and not has_parse_error:
        surf_cmd_line = "python %s %d '%s' > %s 2>&1" % (sc['fileio']['gndalert1'],
            obv['site_id'], obv['ts'], sc['fileio']['surfscriptlogs'])
        p = subprocess.Popen(surf_cmd_line, stdout=subprocess.PIPE, shell=True, 
            stderr=subprocess.STDOUT)

    return not has_parse_error

def check_number_in_users(num):
   

    query = "select user_id from user_mobile where sim_num = '%s'" % (num)

    sc = mem.server_config()

    user_id = dynadb.read(query, 'cnin', sc["resource"]["smsdb"])

    print user_id

    return user_id


def parse_all_messages(args,allmsgs=[]):
    """
       - Processing all messages that came from smsinbox_(loggers/users) and select parsing method dependent to sms message .
      
      :param args: arguement list of modes and criteria of sms message.
      :param allmsgs: list of all messages that being selected from loggers and users table.
      :type args: obj
      :type allmsgs: array
      :returns: **read_success_list, read_fail_list** (*array*)- list of  success and fail message parse.  

     
    """
    read_success_list = []
    read_fail_list = []

    print "table:", args.table
   
    ref_count = 0

    if allmsgs==[]:
        print 'Error: No message to Parse'
        sys.exit()

    total_msgs = len(allmsgs)

    sc = mem.server_config()
    mc = mem.get_handle()
    table_sim_nums = mc.get('%s_mobile_sim_nums' % args.table[:-1])

    while allmsgs:
        is_msg_proc_success = True
        print '\n\n*******************************************************'

        sms = allmsgs.pop(0)
        ref_count += 1

        if args.table == 'loggers':
            # start of sms parsing

            if re.search("^[A-Z]{3}X[A-Z]{1}\*L\*",sms.msg):
                is_msg_proc_success = uts.parse_extensometer_uts(sms)
            elif re.search("\*FF",sms.msg) or re.search("PZ\*",sms.msg):
                is_msg_proc_success = process_piezometer(sms)
            # elif re.search("[A-Z]{4}DUE\*[A-F0-9]+\*\d+T?$",sms.msg):
            elif re.search("[A-Z]{4}DUE\*[A-F0-9]+\*.*",sms.msg):
                df_data = subsurface.v1(sms)
                if df_data:
                    print df_data[0].data ,  df_data[1].data
                    dynadb.df_write(df_data[0])
                    dynadb.df_write(df_data[1])
                    tsm_name = df_data[0].name.split("_")
                    tsm_name = str(tsm_name[1])
                    timestamp = df_data[0].data.reset_index()
                    timestamp = str(timestamp['ts'][0])
                    spawn_alert_gen(tsm_name,timestamp)
                else:
                    print '>> Value Error'
                    is_msg_proc_success = False
              
            elif re.search("^[A-Z]{4,5}\*[xyabcXYABC]\*[A-F0-9]+\*[0-9]+T?$",
                sms.msg):
                try:
                    df_data = subsurface.v2(sms)
                    if df_data:
                        print df_data.data
                        dynadb.df_write(df_data)
                        tsm_name = df_data.name.split("_")
                        tsm_name = str(tsm_name[1])
                        timestamp = df_data.data.reset_index()
                        timestamp = str(timestamp['ts'][0])
                        spawn_alert_gen(tsm_name,timestamp)
                    else:
                        print '>> Value Error'
                        is_msg_proc_success = False

                except IndexError:
                    print "\n\n>> Error: Possible data type error"
                    print sms.msg
                    is_msg_proc_success = False
                except ValueError:
                    print ">> Value error detected"
                    is_msg_proc_success = False
                except MySQLdb.ProgrammingError:
                    print ">> Error writing data to DB"
                    is_msg_proc_success = False
                    
            elif re.search("[A-Z]{4}\*[A-F0-9]+\*[0-9]+$",sms.msg):
                df_data =subsurface.v1(sms)
                if df_data:
                    print df_data[0].data ,  df_data[1].data
                    dynadb.df_write(df_data[0])
                    dynadb.df_write(df_data[1])
                    tsm_name = df_data[0].name.split("_")
                    tsm_name = str(tsm_name[1])
                    timestamp = df_data[0].data.reset_index()
                    timestamp = str(timestamp['ts'][0])
                    spawn_alert_gen(tsm_name,timestamp)
                else:
                    print '>> Value Error'
                    is_msg_proc_success = False
            #check if message is from rain gauge
            elif re.search("^\w{4},[\d\/:,]+",sms.msg):
                df_data = rain.v3(sms)
                if df_data:
                    print df_data.data
                    dynadb.df_write(df_data)
                else:
                    print '>> Value Error'
            elif re.search("ARQ\+[0-9\.\+/\- ]+$",sms.msg):
                df_data = rain.rain_arq(sms)
                if df_data:
                    print df_data.data
                    dynadb.df_write(df_data)
                else:
                    print '>> Value Error'

            elif (sms.msg.split('*')[0] == 'COORDINATOR' or 
                sms.msg.split('*')[0] == 'GATEWAY'):
                is_msg_proc_success = process_gateway_msg(sms)
            elif common_logger_sms(sms) > 0:
                print 'inbox_id: ', sms.inbox_id
                print 'match'
            else:
                print '>> Unrecognized message format: '
                print 'NUM: ' , sms.sim_num
                print 'MSG: ' , sms.msg
                is_msg_proc_success = False


        elif args.table == 'users':
            if re.search("EQINFO",sms.msg.upper()):
                data_table = parser.eq(sms)
                if data_table:
                    dynadb.df_write(data_table)
                else:
                    is_msg_proc_success = False
            elif re.search("^SANDBOX ACK \d+ .+",sms.msg.upper()):
                is_msg_proc_success = amsg.process_ack_to_alert(sms)   
            elif re.search("^ *(R(O|0)*U*TI*N*E )|(EVE*NT )", sms.msg.upper()):
                is_msg_proc_success = process_surficial_observation(sms)                  
            else:
                print "User SMS not in known template.", sms.msg
                is_msg_proc_success = True

        else:
            raise ValueError("Table value not recognized (%s)" % (args.table))
            sys.exit()

            
        if is_msg_proc_success:
            read_success_list.append(sms.inbox_id)
        else:
            read_fail_list.append(sms.inbox_id)

        print ">> SMS count processed:", ref_count

        # method for updating the read_status all messages that have been processed
        # so that they will not be processed again in another run
        if ref_count % 200 == 0:
            smstables.set_read_status(read_success_list, read_status = 1,
                table = args.table, host = args.dbhost)
            smstables.set_read_status(read_fail_list, read_status = -1,
                table = args.table, host = args.dbhost)

            read_success_list = []
            read_fail_list = []

    smstables.set_read_status(read_success_list, read_status = 1,
        table = args.table, host = args.dbhost)
    smstables.set_read_status(read_fail_list, read_status = -1,
        table = args.table, host = args.dbhost)
        
def get_router_ids():
    """
       - Select Router id from loggers table
      
      :parameter: N/A
      :returns: **nums **.(*obj*) - list of keys and values from model_id table;
     
    """
    db, cur = dynadb.connect()

    query = ("SELECT `logger_id`,`logger_name` from `loggers` where `model_id`"
        " in (SELECT `model_id` FROM `logger_models` where "
        "`logger_type`='router') and `logger_name` is not null")

    nums = dynadb.read(query,'get_router_ids')
    nums = {key: value for (value, key) in nums}

    return nums
        
def process_gateway_msg(sms):
    """
       - Processing the gateway message parser for sms data and save data to database.
      
      :param sms: list data info of sms message .
      :type msg: list
      :returns: **True or False ** - Return False for fail to parse message .
     
    """
    print ">> Coordinator message received"
    print sms.msg
    
    # dbio.create_table("coordrssi","coordrssi")

    routers = get_router_ids()
    
    sms.msg = re.sub("(?<=,)(?=(,|$))","NULL",sms.msg)
    
    try:
        datafield = sms.msg.split('*')[1]
        timefield = sms.msg.split('*')[2]
        timestamp = dt.strptime(timefield,
            "%y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        
        smstype = datafield.split(',')[0]
        # process rssi parameters
        if smstype == "RSSI":
            site_name = datafield.split(',')[1]
            rssi_string = datafield.split(',',2)[2]
            print rssi_string
            # format is
            # <router name>,<rssi value>,...
            query = ("INSERT IGNORE INTO router_rssi "
                "(ts, logger_id, rssi_val) VALUES ")
            tuples = re.findall("[A-Z]+,\d+",rssi_string)
            count = 0
            for item in tuples:
                try:
                    query += "('%s',%d,%s)," % (timestamp,
                        routers[item.split(',')[0].lower()], item.split(',')[1])
                    count += 1
                except KeyError:
                    print 'Key error for', item
                    continue
                
            query = query[:-1]

            # print query
            
            if count != 0:
                print 'count', count
                dynadb.write(query, 'process_gateway_msg')
            else:
                print '>> no data to commit'
            return True
        else:
            print ">> Processing coordinator weather"
    except IndexError:
        print "IndexError: list index out of range"
        logger_response(sms,14,True)
    except:
        print ">> Unknown Error", sms.msg
        return False

def get_arguments():
    """
       -The function that checks the argument that being sent from main function and returns the
        arguement of the function.
      
      :parameters: N/A
      :returns: **args** - Mode of action from running python **-db,-ns,-b,-r,-l,-s,-g,-m,-t**.
         
         :Example Input: **-db gsm2 -l 5000 -s 1 -t loggers**

             **args.dbhost**
                -*Database host it can be (local or gsm2)*

                :Example Output: *gsm2*
             **args.table**
                -*Smsinbox table (loggers or users)*

                :Example Output: *loggers* 
             **args.mode**
                -*Mode id* 
             **args.gsm**
                -*GSM name (globe1, smart1, globe2, smart2)*
             **args.status**
                -*GSM status of inbox/outbox*
             **args.messagelimit**
                -*Number of message to read in the process*
             **args.runtest**
                -*Default value False. Set True when running a test in the process*
             **args.bypasslock**
                -*Default value False.*
             **args.nospawn**
                -*Default value False.*
    """
    parser = argparse.ArgumentParser(description = ("Run SMS parser\n "
        "smsparser [-options]"))
    parser.add_argument("-db", "--dbhost", 
        help="host name (check senslope-server-config.txt")
    parser.add_argument("-t", "--table", help="smsinbox table")
    parser.add_argument("-m", "--mode", help="mode to run")
    parser.add_argument("-g", "--gsm", help="gsm name")
    parser.add_argument("-s", "--status", help="inbox/outbox status", type=int)
    parser.add_argument("-l", "--messagelimit", 
        help="maximum number of messages to process at a time", type=int)
    parser.add_argument("-r", "--runtest", 
        help="run test function", action="store_true")
    parser.add_argument("-b", "--bypasslock", 
        help="bypass lock script function", action="store_true")
    parser.add_argument("-ns", "--nospawn", 
        help="do not spawn alert gen", action="store_true")
    
    try:
        args = parser.parse_args()

        if args.status == None:
            args.status = 0
        if args.messagelimit == None:
            args.messagelimit = 200
        if args.dbhost == None:
            args.dbhost = 'local'
        return args        
    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()

def main():
    """
        **Description:**
          - Runs the whole smsparser with the logic of parsing sms txt of users and loggers.

                 :Example Input: **-db gsm2 -l 5000 -s 1 -t loggers**

             **-db**
                -*Database host it can be (local or gsm2)*

                :Example Output: *gsm2*
             **-t**
                -*Smsinbox table (loggers or users)*

                :Example Output: *loggers* 
             **-m**
                -*Mode id* 
             **-g**
                -*GSM name (globe1, smart1, globe2, smart2)*
             **-s**
                -*GSM status of inbox/outbox*
             **-l**
                -*Number of message to read in the process*
             **-r**
                -*Default value False. Set True when running a test in the process*
             **-b**
                -*Default value False.*
             **-ns**
                -*Default value False.*

        .. note:: To run in terminal **python smsparser.py ** with arguments (** -db,-ns,-b,-r,-l,-s,-g,-m,-t**).
    """

    args = get_arguments()

    if not args.bypasslock:
        lockscript.get_lock('smsparser %s' % args.table)

    # dbio.create_table("runtimelog","runtime")
    # logRuntimeStatus("procfromdb","startup")

    print 'SMS Parser'

    print args.dbhost, args.table, args.status, args.messagelimit
    allmsgs = smstables.get_inbox(host=args.dbhost, table=args.table,
        read_status=args.status, limit=args.messagelimit)
    
    if len(allmsgs) > 0:
        msglist = []
        for inbox_id, ts, sim_num, msg in allmsgs:
            sms_item = smsclass.SmsInbox(inbox_id, msg, sim_num, str(ts))
            msglist.append(sms_item)
         
        allmsgs = msglist

        try:
            parse_all_messages(args,allmsgs)
        except KeyboardInterrupt:
            print '>> User exit'
            sys.exit()

    else:
        print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
        return

if __name__ == "__main__":
    main()
    
