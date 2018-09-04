""" Running mainserver scripts    """

import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import multiprocessing
import math
import memcache
import argparse
import volatile.memory as mem
import volatile.static as static
import dynadb.db as db
import smstables
import modem.modem as modem

def check_id_in_table(table,gsm_id):
    """
    - The process of selecting data from user or logger mobile with mobile id.
         
    :param table: table name of the gsm_id.
    :param gsm_id: gsm_id of the recipient.
    :type table: int 
    :type gsm_id: int

    Returns: 
        obj: recipient number (*int*)
    """
    query = "select * from %s_mobile where mobile_id='%s' limit 80"%(table[:-1],gsm_id)
    query_number = db.read(query,'check id in table') 
    if len(query_number.sim_num) != 0:
        return query_number[0][0]
    else:
        print " >> gsm id doesn't exist"
        
def log_runtime_status(script_name,status):
    """
    - The process of logging runtime status is a function that log the runtime of script if  alive or not.
     
    :param script_name: Script file name.
    :param status: script runtime status.
    :type script_name: str
    :type status: str

    """
    sc = mem.server_config()
    gsm_modules_host = sc["resource"]["smsdb"]

    if (status == 'alive'):
        ts = dt.today()
        diff = (ts.minute%10) * 60 + ts.second
        ts = ts - td(seconds=diff)
        logtimestamp = ts.strftime("%Y-%m-%d %H:%M:00")
    else:
        logtimestamp = dt.today().strftime("%Y-%m-%d %H:%M:00")
    
    print ">> Logging runtime '" + status + "' at " + logtimestamp 
    
    query = ("insert ignore into runtimelog (ts, script_name, log_details) "
        "Values ('%s','%s','%s')") % (logtimestamp, script_name, status)
    
    db.write(query, 'log_runtime_status', False,gsm_modules_host)
    """
        **Description:**
          - Log the runtime of script ro know its status.
         
        :param script_name: Script file name.
        :param status: script runtime status.
        :type script_name: str
        :type status: str
        :returns: N/A
    """       
    
def get_allowed_prefixes(network):
    """
    - The process of checking network prefixes extensions in the volatile.memory funtion server_config.
         
    :param netwok: Table name (users and loggers) and **Default** to **users** table .
    :type network: str
    Returns: 
        int: Outputs the number with extended 639 or 09 in the number.
    """
    sc = mem.server_config()
    if network.upper() == 'SMART':
        prefix_list = sc["simprefix"]["smart"].split(',')
    else:
        prefix_list = sc["simprefix"]["globe"].split(',')

    extended_prefix_list = []
    for p in prefix_list:
        extended_prefix_list.append("639"+p)
        extended_prefix_list.append("09"+p)

    return extended_prefix_list
    
def send_messages_from_db(gsm = None, table = 'users', send_status = 0, 
    gsm_info = None, limit = 10):
    """
    - The process getting all message unset and try to send the message again.
     
    :param table: Table name and **Default** to **users** table .
    :param send_status: the id number of the gsm message status and  **Default** to **0**.
    :param gsm_id: The id of the gsm that is use globe (2,4) and smart (3,5) and **Default** to **0**.
    :param limit: The limit of message to get in the table and **Default** to **10**.
    :type table: str
    :type send_status: str
    :type gsm_id: int
    :type limit: int
    """
    if gsm == None:
        raise ValueError("No gsm instance defined")

    sc = mem.server_config()
    host = sc['resource']['smsdb']

    allmsgs = smstables.get_all_outbox_sms_from_db(table, send_status, 
        gsm_info["id"], limit)
    if len(allmsgs) <= 0:
        return
    
    print ">> Sending messagess from db"

    table_mobile = static.get_mobiles(table=table, resource="sms_data")
    inv_table_mobile = {v: k for k, v in table_mobile.iteritems()}
    # print inv_table_mobile
        
    msglist = []
    error_stat_list = []
    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    for stat_id, mobile_id, outbox_id, gsm_id, sms_msg in allmsgs:
        try:
            smsItem = modem.GsmSms(stat_id, inv_table_mobile[mobile_id], sms_msg,'')
            msglist.append([smsItem, gsm_id, outbox_id, mobile_id])

        except KeyError:
            print ">> Unknown mobile_id:", mobile_id
            error_stat_list.append((stat_id,-1,today,gsm_id,outbox_id,mobile_id))
        
            continue

    if len(error_stat_list) > 0:
        print ">> Ignoring invalid messages...",
        smstables.set_send_status(table, error_stat_list)
        print "done"

    if len(msglist) == 0:
        print ">> No valid message to send"
        return
        
    allmsgs = msglist

    status_list = []
    
    allowed_prefixes = get_allowed_prefixes(gsm_info["network"])

    # # cycle through all messages
    for msg in allmsgs:

        try:
            num_prefix = re.match("^ *((0)|(63))9\d\d",msg[0].simnum).group()
            num_prefix = num_prefix.strip()
        except:
            print 'Error getting prefix', msg[0].simnum
            continue
            # check if recepient number in allowed prefixed list    
        if num_prefix in allowed_prefixes:
            ret = gsm.send_msg(msg[0].data, msg[0].simnum.strip())
            today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
            if ret:
                send_stat = 1
                stat = msg[0].num,1,today,msg[1],msg[2],msg[3]
            else:
                stat = msg[0].num,5,today,msg[1],msg[2],msg[3]

            status_list.append(stat)
            
        else:
            print "Number not in prefix list", num_prefix
            today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
            stat = msg[0].num,-1,today,msg[1],msg[2],msg[3]
            status_list.append(stat)
            continue

    smstables.set_send_status(table, status_list, host)

    
    #Get all outbox messages with send_status "SENT" and attempt to send
    #   chatterbox acknowledgements
    #   send_status will be changed to "SENT-WSS" if successful
    # dsll.sendAllAckSentGSMtoDEWS()    
    
def try_sending_messages(gsm, gsm_info):
    """
    - The process of resending messages that have read_status **1 to 5** for smsinbox_(**loggers or users**).
     
    :param gsm_id: gsm_id of 4 (globe) and 5 (smart).
    :type gsm_id: int

    """
    # print ">> eavm: skipping.."
    # time.sleep(30)
    # return
    start = dt.now()
    while True:  
        # send_messages_from_db(network)
        send_messages_from_db(gsm, table = 'users', send_status = 5,
            gsm_info = gsm_info)
        send_messages_from_db(gsm, table = 'loggers', send_status = 5, 
            gsm_info = gsm_info)
        print '.',
        time.sleep(5)
        if (dt.now()-start).seconds > 30:
            break

def simulate_gsm(network='simulate'):
    """
    - The process simulate is a function that runs a gsm simulation of insert,update and check status of gsm.
     
    :param network: gsm network and **Default** to **simulate**.
    :type table: str

    """
    print "Simulating GSM"

    sc = mem.server_config()
    sms_mirror_host = sc["resource"]["sms_mirror_db"]
    mobile_nums_db = sc["resource"]["mobile_nums_db"]
    smsdb_host = sc["resource"]["smsdb"]
    
    smsinbox_sms = []

    query = ("select sms_id, timestamp, sim_num, sms_msg from smsinbox "
        "where web_flag not in ('0','-1') limit 1000")

    smsinbox_sms = db.read(query=query, resource="sensor_data")

    logger_mobile_sim_nums = static.get_mobiles('loggers', mobile_nums_db)
    user_mobile_sim_nums = static.get_mobiles('users', mobile_nums_db)

    gsm_id = 1
    loggers_count = 0
    users_count = 0
    
    ts_stored = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    query_loggers = ("insert into smsinbox_loggers (ts_sms, ts_stored, "
        "mobile_id, sms_msg,read_status,gsm_id) values ")
    query_users = ("insert into smsinbox_users (ts_sms, ts_stored, mobile_id, "
        "sms_msg,read_status,gsm_id) values ")

    sms_id_ok = []
    sms_id_unk = []
    ts_sms = 0
    ltr_mobile_id= 0

    for m in smsinbox_sms:
        ts_sms = m[1]
        sms_msg = m[3]
        read_status = 0 
    
        if m[2] in logger_mobile_sim_nums.keys():
            query_loggers += "('%s','%s',%d,'%s',%d,%d)," % (ts_sms, ts_stored,
                logger_mobile_sim_nums[m[2]], sms_msg, read_status, gsm_id)
            ltr_mobile_id= logger_mobile_sim_nums[m[2]]
            loggers_count += 1
        elif m[2] in user_mobile_sim_nums.keys():
            query_users += "('%s','%s',%d,'%s',%d,%d)," % (ts_sms, ts_stored,
                user_mobile_sim_nums[m[2]], sms_msg, read_status, gsm_id)
            users_count += 1
        else:            
            # print 'Unknown number', m[2]
            sms_id_unk.append(m[0])
            continue
        
        sms_id_ok.append(m[0])

    query_loggers = query_loggers[:-1]
    query_users = query_users[:-1]

    print ("Copying %d loggers item and %d users item with %d "
        "unknown") % (loggers_count, users_count, len(sms_id_unk))
    
    if len(sms_id_ok)>0:

        if loggers_count > 0:
            db.write(query=query_loggers, resource="sms_data")

        if users_count > 0:
            db.write(query=query_users, resource="sms_data")
        
        sms_id_ok = str(sms_id_ok).replace("L","")[1:-1]
        query = ("update smsinbox set web_flag = '0' "
            "where sms_id in (%s);") % (sms_id_ok)
        db.write(query=query, resource="sensor_data")

    if len(sms_id_unk)>0:
        # print sms_id_unk
        sms_id_unk = str(sms_id_unk).replace("L","")[1:-1]
        query = ("update smsinbox set web_flag = '-1' "
            "where sms_id in (%s);") % (sms_id_unk)
        db.write(query=query, resource="sensor_data")

    sys.exit()

def log_csq(gsm, gsm_id):
    """
    - The process that logs the gsm signal of the gsm id .
     
    :param gsm: List data of gsm.
    :param gsm_id: Id of the gsm in the database table.
    :type gsm: list
    :type gsm_id: int
    
    Returns:
        int: Signal value of gsm module.

    """
        
    ts_today = dt.today().strftime('%Y-%m-%d %H:%M:%S')

    csq_val = gsm.csq()

    query = ("insert into gsm_csq_logs (`ts`,`gsm_id`,`csq_val`) "
        "values ('%s', %d, %d)") % (ts_today, gsm_id, csq_val)

    db.write(query = query, identifier = "", last_insert = False, 
        host = "local")

    return csq_val 

        
def run_server(gsm_mod, gsm_info, table='loggers'):
    """
    - The process of running the gsm server to read the recieved message and to try to 
      send message from outbox.It checks gsm info **(name, network, id)** to read messages 
      from inbox and outbox of the table.
         
    :param gsm_info: id of the gsm server.
    :param table: table name to use and **Default** to **loggers**
    :type table: str
    :type gsm_info: int
    :type table: str

    """
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    lastAlertMsgSent = ''
    logruntimeflag = True
    checkIfActive = True

    gsm_mod.set_defaults() 
            
    log_runtime_status(gsm_info["name"],"startup")
    
    print '**' + gsm_info['name'] + ' GSM server active**'
    print time.asctime()
    network = gsm_info['name'].upper()
    print "CSQ:", log_csq(gsm_mod, gsm_info['id'])

    while True:
        m = gsm_mod.count_msg()
        if m>0:
            allmsgs = gsm_mod.get_all_sms(network)

            try:
                smstables.write_inbox(allmsgs,gsm_info)
            except KeyboardInterrupt:
                print ">> Error: May be an empty line.. skipping message storing"
            
            gsm_mod.delete_sms(gsm_info["module"])
                
            print dt.today().strftime("\n" + network 
                + " Server active as of %A, %B %d, %Y, %X")

            print "CSQ:", log_csq(gsm_mod, gsm_info['id'])

            log_runtime_status(gsm_info["name"],"alive")

            try_sending_messages(gsm_mod, gsm_info)
            
        elif m == 0:
            
            try_sending_messages(gsm_mod, gsm_info)
            
            today = dt.today()
            if (today.minute % 10 == 0):
                if checkIfActive:
                    print "\n", network, today.strftime("Server active as of "
                        "%A, %B %d, %Y, %X")
                    print "CSQ:", log_csq(gsm_mod, gsm_info['id'])
                    log_runtime_status(gsm_info["name"],"alive")
                checkIfActive = False
            else:
                checkIfActive = True
                
        elif m == -1:
            serverstate = 'inactive'
            # log_runtime_status(network,"gsm inactive")
            raise modem.ResetException("GSM MODULE MAYBE INACTIVE")

        elif m == -2:
            raise modem.ResetException("Error in parsing mesages: No data returned by GSM")
        else:
            raise modem.ResetException("Error in parsing mesages: Error unknown")

def get_arguments():
    """
    - The process that checks the argument that being sent from main function and returns the arguement list of function.
         
    Returns: 
        dict : Dictionary out of mode of action from running python when it reads the following.

           
    Example Output::

            >> print args.table
                loggers # Smsinbox table (loggers or users)
            >> print args.network
                smart  # Network name ( smart/globe/simulate )
            >> print args.gsm_id
                1 # GSM id ( 1, 2 ,3... )

    """
    parser = argparse.ArgumentParser(description="Run SMS server [-options]")
    parser.add_argument("-t", "--table", 
        help="smsinbox table (loggers or users)")
    parser.add_argument("-n", "--network", 
        help="network name (smart/globe/simulate)")
    parser.add_argument("-g", "--gsm_id", type = int,
        help="gsm id (1,2,3...)")
    
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

def get_gsm_modules(reset_val = False):
    """
    -  The process of  getting the gsm modules information.
     
    :param reset_val: Trigger value to check the gsm information and **Default** to **False**
    :type table: boolean, default False

    Returns:
       obj: Gsm info output for success processing.

    Example Output::

        {1: {'network': 'globe', 'module': 0, 'port': 'dummy', 'num': '639171234567', 
        'pwr_on_pin': 1, 'ring_pin': 0, 'id': 1, 'name': 'simulate'}, 2: {'network': 
        'globe', 'module': 1, 'port': None, 'num': '639176321023', 'pwr_on_pin': 0, 
        'ring_pin': 0, 'id': 2, 'name': 'globe'}, 3: {'network': 'smart', 'module': 1, 
        'port': None, 'num': '639988448687', 'pwr_on_pin': 0, 'ring_pin': 0, 'id': 3, 
        'name': 'smart'}, 4: {'network': 'globe', 'module': 2, 'port': '/dev/globeport', 
        'num': '639175388301', 'pwr_on_pin': 11, 'ring_pin': 15, 'id': 4, 
        'name': 'globe1'}, 5: {'network': 'smart', 'module': 2, 'port': '/dev/smartport',
        'num': '639088125640', 'pwr_on_pin': 31, 'ring_pin': 33, 'id': 5, 'name': 'smart1'},
        6: {'network': 'globe', 'module': 2, 'port': '/dev/smartport', 'num': '639175963573',
        'pwr_on_pin': 31, 'ring_pin': 33, 'id': 6, 'name': 'globe2'}, 7: {'network': 'smart',
        'module': 2, 'port': '/dev/ttyUSB1', 'num': '639088125638', 'pwr_on_pin': 29,
        'ring_pin': 33, 'id': 7, 'name': 'smart1'}}
    """
    try:
        DF_GSM_MODULES = mem.get("DF_GSM_MODULES")
    except AttributeError:
        raise AttributeError("No DF_GSM_MODULES in memory. Initialize first.")
   
    # backwards compatibility
    DF_GSM_MODULES.rename(
        columns={
            "network_type": "network",
            "gsm_name": "name",
            "gsm_sim_num": "num",
            "ser_port": "port",
            "gsm_id": "id",
            "module_type": "module"
        }, inplace = True)

    gsm_modules = DF_GSM_MODULES.to_dict(orient='index')

    return gsm_modules

def main():
    """
    -  The process of running the whole gsmserver by checking if the gsmserver arguement is being initialize as gsm_id, table or network with respective agruement id.

    .. todo::

        1. To run the script **open** a terminal or bash 
        2. Set your terminal/bash path to **/centraserver/gsm/** .
        3. Type inside the terminal/bash  ""**python gsmserver.py** **-g** 
           *<gsm id (1,2,3...)>* **-t** *<smsinbox table (loggers or users)>* 
           **-n** *<network name (smart/globe/simulate)>* "".
        4. Click Enter
       
    """ 

    args = get_arguments()
    
    gsm_modules = get_gsm_modules()

    args.gsm_id = args.gsm_id - 1

    if args.gsm_id not in gsm_modules.keys():
        print ">> Error in gsm module selection (%s)" % (args.gsm_id) 
        sys.exit()

    if gsm_modules[args.gsm_id]["port"] is None:
        print ">> Error: missing information on gsm_module"
        sys.exit()
    
    print 'Running gsm server ...'

    gsm_info = gsm_modules[args.gsm_id]
    gsm_info["pwr_on_pin"] = int(gsm_info["pwr_on_pin"])
    gsm_info["ring_pin"] = int(gsm_info["ring_pin"])
    gsm_info["id"] = int(gsm_info["id"]) 

    if gsm_info['name'] == 'simulate':
        simulate_gsm(gsm_info['network'])
        sys.exit()

    sc = mem.server_config()
    gsm = None

    try:
        gsm = modem.GsmModem(gsm_info['port'], sc["serial"]["baudrate"], 
            gsm_info["pwr_on_pin"], gsm_info["ring_pin"])
    except serial.SerialException:
        print '**NO COM PORT FOUND**'
        serverstate = 'serial'
        raise ValueError(">> Error: no com port found")

    try:
        run_server(gsm, gsm_info)
    except modem.ResetException:
        print "> Resetting system because of GSM failure"
        gsm.reset()
        sys.exit()
    
if __name__ == '__main__':
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print 'Bye'
            break

