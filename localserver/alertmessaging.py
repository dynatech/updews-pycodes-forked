import os,time,re,sys
import MySQLdb
import datetime
import cfgfileio as cfg
from datetime import datetime as dt
from datetime import timedelta as td
import serverdbio as dbio
import mainserver as server
import queryserverinfo as qsi
import argparse
#---------------------------------------------------------------------------------------------------------------------------

def check_alert_message():
    c = cfg.config()    
    dbio.create_table("runtimelog","runtime")
    server.log_runtime_status("alert","checked")

    alertfile = cfg.config().fileio.allalertsfile
    f = open(alertfile,'r')
    alertmsg = f.read()
    f.close()

    print alertmsg
    if not alertmsg:
        print '>> No alert msg read.'
        return

    # write alert message to db
    server.write_alert_to_db(alertmsg)

def get_alert_staff_numbers():
    query = """select nickname, numbers from dewslcontacts where grouptags like '%alert%'"""
    contacts = dbio.querydatabase(query,'checkalert')
    return contacts

def send_alert_message():
    # check due alert messages
    ts_due = dt.today()
    query = "select alert_id,alertmsg from smsalerts where ack = 'none' and ts_set <= '%s'" % (ts_due.strftime("%Y-%m-%d %H:%M:%S"))

    alertmsg = dbio.querydatabase(query,'send_alert_message')

    if alertmsg == None:
        print 'No alertmsg set for sending'
        return

    message = 'Alert ID %d:\n%s\n' % (alertmsg[0][0],alertmsg[0][1])
    message += 'Text "ACK <alert id> <valid/invalid> <remarks>" to acknowledge'

    # send to alert staff
    contacts = get_alert_staff_numbers()
    for item in contacts:
        # for multile contacts
        for i in item[1].split(','):
            server.write_outbox_message_to_db(message,i)
    
    # set alert to 15 mins later
    ts_due = ts_due + td(seconds=60*15)
    query = "update smsalerts set ts_set = '%s' where alert_id = %s" % (ts_due.strftime("%Y-%m-%d %H:%M:%S"),alertmsg[0][0])

    dbio.commit_to_db(query, 'checkalertmsg')


def process_ack_to_alert(msg):
    try:
        alert_id = re.search("(?<=K )\d+(?= )",msg.data,re.IGNORECASE).group(0)
    except:
        errmsg = "Error in parsing alert id. Please try again"
        server.write_outbox_message_to_db(errmsg,msg.simnum)
        return True

    # check to see if message from chatter box
    try:
        name = qsi.get_name_of_staff(msg.simnum)
        if re.search("server",name.lower()):
            name = re.search("(?>=-).+(?= from)").group(0)
    except:
        try:
            chat_footer = re.search("-[A-Za-z ]+ from .+$",msg.data).group(0)
            name = re.search("(?<=-)[A-Za-z]+(?= )",chat_footer).group(0)
            msg.data = msg.data.replace(chat_footer,"")
        except:
            errmsg = "You are not permitted to acknowledge."
            server.write_outbox_message_to_db(errmsg,msg.simnum)
            return True

    try:
        remarks = re.search("(?<=\d ).+(?=($|\r|\n))",msg.data,re.IGNORECASE).group(0)
    except:
        errmsg = "Please put in your remarks."
        server.write_outbox_message_to_db(errmsg,msg.simnum)
        return True

    try:
        alert_status = re.search("(in)*valid(ating)*",remarks,re.IGNORECASE).group(0)
        remarks = remarks.replace(alert_status,"").strip()
    except:
        errmsg = "Please put in the alert status validity. i.e (VALID, INVALID, VALIDATING)"
        server.write_outbox_message_to_db(errmsg,msg.simnum)
        return True

    query = "update smsalerts set ack = '%s', ts_ack = '%s', remarks = '%s', alertstat = '%s' where alert_id = %s" % (name,msg.dt,remarks,alert_status, alert_id)
    dbio.commit_to_db(query,process_ack_to_alert)

    contacts = get_alert_staff_numbers()
    message = "Alert ID %s ACK by %s on %s\nStatus: %s\nRemarks: %s" % (alert_id,name,msg.dt,alert_status,remarks)
    
    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    for item in contacts:
        message = message.replace("ALERT","AL3RT")
        server.write_outbox_message_to_db(message,item[1])

    return True

def update_shift_tags():
    # remove tags to old shifts
    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    print 'Updating shift tags for', today

    query = "update senslopedb.dewslcontacts set grouptags = replace(grouptags,',alert-mon','') where grouptags like '%alert-mon%'"
    dbio.commit_to_db(query, 'update_shift_tags')

    # update the tags of current shifts
    query = """
        update dewslcontacts as t1,
        ( select timestamp,iompmt,iompct,oomps,oompmt,oompct from monshiftsched 
          where timestamp < '%s' 
          order by timestamp desc limit 1
        ) as t2
        set t1.grouptags = concat(t1.grouptags,',alert-mon')
        where t1.nickname = t2.iompmt or
        t1.nickname = t2.iompct or
        t1.nickname = t2.oomps or
        t1.nickname = t2.oompmt or
        t1.nickname = t2.oompct
    """ % (today)
    dbio.commit_to_db(query, 'update_shift_tags')

def main():
    parser = argparse.ArgumentParser(description="Request information from server\n PSIR [-options]")
    parser.add_argument("-w", "--writetodb", help="write alert to db", action="store_true")
    parser.add_argument("-c", "--check_alert_message", help="check alert messages from db", action="store_true")
    parser.add_argument("-s", "--send_alert_message", help="send alert messages from db", action="store_true")
    parser.add_argument("-u", "--updateshifts", help="update shifts with alert tag", action="store_true")
    parser.add_argument("-cs", "--checksendalert", help="check alert then send", action="store_true")
    
    
    try:
        args = parser.parse_args()
    except:
        print '>> Error in parsing'
        error = parser.format_help()
        print error
        return

    if args.writetodb: 
        writetodb = True
    if args.check_alert_message:
        check_alert_message()
    if args.send_alert_message:
        send_alert_message()
    if args.checksendalert:
        check_alert_message()
        send_alert_message()
    if args.updateshifts:
        update_shift_tags()
    
if __name__ == "__main__":
    main()
