import os,time,re,sys
import MySQLdb
import datetime
import cfgfileio as cfg
from datetime import datetime as dt
from datetime import timedelta as td
import serverdbio as dbio
import gsmserver as server
import queryserverinfo as qsi
import argparse
#---------------------------------------------------------------------------------------------------------------------------

def get_alert_staff_numbers():
    query = ("select t1.user_id,t2.sim_num from user_alert_info t1 inner join"
        " user_mobile t2 on t1.user_id = t2.user_id where t1.send_alert = 1;")

    contacts = dbio.query_database(query,'checkalert')
    return contacts

def write_outbox_dyna(msg,num):
    ts_written = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    
    smart_prefixes = server.get_allowed_prefixes('SMART')
    globe_prefixes = server.get_allowed_prefixes('GLOBE')

    try:
        num_prefix = re.match("^((0)|(63))9\d\d",num).group()
    except:
        print '>> Unable to send sim number in this gsm module'
        return -1

    if num_prefix in smart_prefixes:
        # return 'SMART'
        gsm_id = 'SMART'
    elif num_prefix in globe_prefixes:
        # return 2
        gsm_id = 'GLOBE'
    else:
        print '>> Prefix', num_prefix, 'cannot be sent'
        return -1

    query = ("insert into smsoutbox (timestamp_written,sms_msg,recepients,"
        "send_status,gsm_id) values "
        "('%s','%s','%s','UNSENT','%s');") % (ts_written,msg,num,gsm_id)

    # print query
    dbio.commit_to_db(query,'wod',False,'gsm') 

def send_alert_message():
    # check due alert messages
    # ts_due = dt.today()
    # query = ("select alert_id, alert_msg from sms_alerts where alert_status is"
    #     " null and ts_set <= '%s'") % (ts_due.strftime("%Y-%m-%d %H:%M:%S"))

    # alertmsg = dbio.query_database(query,'send_alert_message')
    alert_msgs = check_alerts()

    if len(alert_msgs) == 0:
        print 'No alertmsg set for sending'
        return

    for stat_id, site_code, trigger_source, alert_symbol, ts_last_retrigger in alert_msgs:
        tlr_str = ts_last_retrigger.strftime("%Y-%m-%d %H:%M:%S")
        message = ("SANDBOX:\n"
            "As of %s\n"
            "Alert ID %d:\n"
            "%s:%s:%s\n\n"
            "Text\nSandbox ACK <alert_id> <validity> <remarks>") % (tlr_str,
            stat_id, site_code, alert_symbol, trigger_source)

        print message
    
        # send to alert staff
        contacts = get_alert_staff_numbers()
        for mobile_id, sim_num in contacts:
            write_outbox_dyna(message, sim_num)
        
        # # set alert to 15 mins later
        ts_due = dt.now() + td(seconds=60*15)
        query = ("update alert_status set ts_set = '%s' where "
            "stat_id = %s") % (ts_due.strftime("%Y-%m-%d %H:%M:%S"),
            stat_id)

        dbio.commit_to_db(query, 'checkalertmsg')

def check_alerts():
    ts_now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    query = ("SELECT stat_id, site_code, trigger_source, "
            "alert_symbol, ts_last_retrigger FROM "
            "(SELECT stat_id, ts_last_retrigger, site_id, "
            "trigger_source, alert_symbol FROM "
            "(SELECT stat_id, ts_last_retrigger, site_id, "
            "trigger_sym_id FROM "
            "(SELECT * FROM alert_status "
            "WHERE ts_set < '%s' " 
            "and ts_ack is NULL "
            ") AS stat "
            "INNER JOIN "
            "operational_triggers AS op "
            "USING (trigger_id) "
            ") AS trig "
            "INNER JOIN "
            "(SELECT trigger_sym_id, trigger_source, "
            "alert_level, alert_symbol FROM "
            "operational_trigger_symbols "
            "INNER JOIN "
            "trigger_hierarchies "
            "USING (source_id) "
            ") as sym "
            "USING (trigger_sym_id)) AS alert "
            "INNER JOIN "
            "sites "
            "USING (site_id)") % (ts_now)

    alert_msgs = dbio.query_database(query,'check_alerts')

    print "alert messages:", alert_msgs

    return alert_msgs

def process_ack_to_alert(msg):
    try:
        stat_id = re.search("(?<=K )\d+(?= )",msg.data,re.IGNORECASE).group(0)
    except:
        errmsg = "Error in parsing alert id. Please try again"
        # server.write_outbox_message_to_db(errmsg,msg.simnum)
        return False

    # check to see if message from chatter box
    # try:
    #     name = qsi.get_name_of_staff(msg.simnum)
    #     if re.search("server",name.lower()):
    #         name = re.search("(?>=-).+(?= from)").group(0)
    # except:
    #     try:
    #         chat_footer = re.search("-[A-Za-z ]+ from .+$",msg.data).group(0)
    #         name = re.search("(?<=-)[A-Za-z]+(?= )",chat_footer).group(0)
    #         msg.data = msg.data.replace(chat_footer,"")
    #     except:
    #         errmsg = "You are not permitted to acknowledge."
    #         server.write_outbox_message_to_db(errmsg,msg.simnum)
    #         return True

    user_id, nickname = qsi.get_name_of_staff(msg.simnum)
    print user_id, nickname, msg.data
    if re.search("server",nickname.lower()):
        try:
            nickname = re.search("(?<=-).+(?= from)", msg.data).group(0)
        except AttributeError:
            print "Error in processing nickname"
    # else:
    #     name = nickname

    try:
        remarks = re.search("(?<=\d ).+(?=($|\r|\n))",msg.data, 
            re.IGNORECASE).group(0)
    except AttributeError:
        errmsg = "Please put in your remarks."
        write_outbox_dyna(errmsg, msg.simnum)
        return True

    try:
        alert_status = re.search("(in)*valid(ating)*", remarks,
            re.IGNORECASE).group(0)
        remarks = remarks.replace(alert_status,"").strip()
    except AttributeError:
        errmsg = ("Please put in the alert status validity."
            " i.e (VALID, INVALID, VALIDATING)")
        write_outbox_dyna(errmsg, msg.simnum)
        return True

    alert_status_dict = {"validating": 0, "valid": 1, "invalid": -1}

    query = ("update alert_status set user_id = %d, alert_status = %d, "
        "ts_ack = '%s', remarks = '%s' where stat_id = %s") % (user_id,
        alert_status_dict[alert_status.lower()], msg.dt, remarks, stat_id)
    # print query
    dbio.commit_to_db(query,process_ack_to_alert)

    contacts = get_alert_staff_numbers()
    message = ("SANDBOX (test ack):\nAlert ID %s ACK by %s on %s\nStatus: %s\n"
        "Remarks: %s") % (stat_id, nickname, msg.dt, alert_status, remarks)
    
    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    for user_id, sim_num in contacts:
        write_outbox_dyna(message,sim_num)
        print message, sim_num

    return True

def update_shift_tags():
    # remove tags to old shifts
    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    print 'Updating shift tags for', today

    query = ("update senslopedb.dewslcontacts set grouptags = "
        "replace(grouptags,',alert-mon','') where grouptags like '%alert-mon%'")
    dbio.commit_to_db(query, 'update_shift_tags')

    # update the tags of current shifts
    query = (
        "update dewslcontacts as t1,"
        "(select timestamp,iompmt,iompct,oomps,oompmt,oompct from monshiftsched"
        "  where timestamp < '%s' "
        "  order by timestamp desc limit 1"
        ") as t2"
        "set t1.grouptags = concat(t1.grouptags,',alert-mon')"
        "where t1.nickname = t2.iompmt or"
        "t1.nickname = t2.iompct or"
        "t1.nickname = t2.oomps or"
        "t1.nickname = t2.oompmt or"
        "t1.nickname = t2.oompct"
        ) % (today)
    dbio.commit_to_db(query, 'update_shift_tags')

def main():
    desc_str = "Request information from server\n PSIR [-options]"
    parser = argparse.ArgumentParser(description=desc_str)
    parser.add_argument("-w", "--writetodb", help="write alert to db", 
        action="store_true")
    parser.add_argument("-s", "--send_alert_message", 
        help="send alert messages from db", action="store_true")
    parser.add_argument("-u", "--updateshifts", 
        help="update shifts with alert tag", action="store_true")
    parser.add_argument("-cs", "--checksendalert", 
        help="check alert then send", action="store_true")
    parser.add_argument("-c", "--check_alerts", 
        help="check alerts", action="store_true")
    
    
    try:
        args = parser.parse_args()
    except:
        print '>> Error in parsing in line arguments'
        error = parser.format_help()
        print error
        return

    if args.writetodb: 
        writetodb = True
    if args.send_alert_message:
        send_alert_message()
    if args.updateshifts:
        update_shift_tags()
    if args.check_alerts:
        check_alerts()

    
if __name__ == "__main__":
    main()
