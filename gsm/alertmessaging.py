import re
from datetime import datetime as dt
from datetime import timedelta as td
import dynadb.db as dbio
import argparse
import smstables
import pandas as pd
#---------------------------------------------------------------------------------------------------------------------------

def get_alert_staff_numbers():
    query = ("select t1.user_id,t2.sim_num,t2.gsm_id from user_alert_info t1 inner join"
        " user_mobile t2 on t1.user_id = t2.user_id where t1.send_alert = 1;")

    contacts = dbio.read(query=query,identifier='checkalert',host="gsm2")
    return contacts

def monitoring_start(site_id, ts_last_retrigger):

    query =  "SELECT ts, ts_updated FROM "
    query += "  (SELECT * FROM public_alerts "
    query += "  WHERE site_id = %s " %site_id
    query += "  AND (ts_updated <= '%s' " %ts_last_retrigger
    query += "    OR (ts_updated >= '%s' " %ts_last_retrigger
    query += "      AND ts <= '%s')) " %ts_last_retrigger
    query += "  ) AS pub "
    query += "INNER JOIN "
    query += "  (SELECT * FROM public_alert_symbols "
    query += "  WHERE alert_type = 'event') AS sym "
    query += "ON pub.pub_sym_id = sym.pub_sym_id "
    query += "ORDER BY ts DESC LIMIT 3"

    # previous positive alert
    prev_pub_alerts = pd.DataFrame(list(dbio.read(query)),
                                      columns=['ts', 'ts_updated'])

    if len(prev_pub_alerts) == 1:
        start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[0])
    # two previous positive alert
    elif len(prev_pub_alerts) == 2:
        # one event with two previous positive alert
        if pd.to_datetime(prev_pub_alerts['ts'].values[0]) - \
                pd.to_datetime(prev_pub_alerts['ts_updated'].values[1]) <= \
                td(hours=0.5):
            start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[1])
        else:
            start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[0])
    # three previous positive alert
    else:
        if pd.to_datetime(prev_pub_alerts['ts'].values[0]) - \
                pd.to_datetime(prev_pub_alerts['ts_updated'].values[1]) <= \
                td(hours=0.5):
            # one event with three previous positive alert
            if pd.to_datetime(prev_pub_alerts['ts'].values[1]) - \
                    pd.to_datetime(prev_pub_alerts['ts_updated'].values[2]) \
                    <= td(hours=0.5):
                start_monitor = pd.to_datetime(prev_pub_alerts['timestamp']\
                        .values[2])
            # one event with two previous positive alert
            else:
                start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[1])
        else:
            start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[0])

    return start_monitor

def rainfall_details(site_id, start_monitor, ts_last_retrigger):
    query =  "SELECT gauge_name FROM "
    query += "  (SELECT * FROM rainfall_alerts "
    query += "  WHERE site_id = %s " %site_id
    query += "  AND ts >= '%s' " %start_monitor
    query += "  AND ts <= '%s' " %ts_last_retrigger
    query += "  ) AS alerts "
    query += "INNER JOIN "
    query += "  rainfall_gauges "
    query += "USING (rain_id) "
    data_source_df = pd.DataFrame(list(dbio.read(query)),
                                  columns=['gauge_name'])
    data_source = ':' + ','.join(set(data_source_df['gauge_name']))
    return data_source

def subsurface_details(site_id, start_monitor, ts_last_retrigger):
    query =  "SELECT node_id, tsm_name FROM "
    query += "  (SELECT * FROM node_alerts "
    query += "  WHERE ts >= '%s' " %start_monitor
    query += "  AND ts <= '%s' " %ts_last_retrigger
    query += "  ) AS alerts "
    query += "INNER JOIN "
    query += "  (SELECT * FROM tsm_sensors "
    query += "  WHERE site_id = '%s' " %site_id
    query += "  ) AS sensors "
    query += "USING (tsm_id)"
    data_source_df = pd.DataFrame(list(dbio.read(query)),
                                  columns=['node', 'tsm_name'])
    data_source_df = data_source_df.drop_duplicates(['node', 'tsm_name'])
    tsm_source_df = data_source_df.groupby('tsm_name', as_index=False)
    data_source = ':'+','.join(tsm_source_df.apply(tsm_details))
    
    return data_source

def tsm_details(df):
    nodes = ','.join(set(df['node'].apply(lambda x: str(x))))
    lst = df['tsm_name'] + '(' + nodes + ')'
    return lst

def surficial_details(site_id, start_monitor, ts_last_retrigger):
    query =  "SELECT marker_name FROM "
    query += "  (SELECT * FROM marker_alerts "
    query += "  WHERE ts >= '%s' " %start_monitor
    query += "  AND ts <= '%s' " %ts_last_retrigger
    query += "  AND alert_level > 0 "
    query += "  ) AS alerts "
    query += "INNER JOIN "
    query += "  (SELECT marker_id, marker_name FROM "
    query += "    (SELECT hist.marker_id, hist.history_id FROM "
    query += "        (SELECT marker_id, MAX(ts) AS ts "
    query += "        FROM marker_history "
    query += "        WHERE event in ('add', 'rename') "
    query += "        GROUP BY marker_id "
    query += "        ) AS M "
    query += "      INNER JOIN "
    query += "        (SELECT * "
    query += "        FROM marker_history "
    query += "        WHERE event in ('add', 'rename') "
    query += "        ) AS hist "
    query += "      USING (marker_id, ts) "
    query += "    WHERE marker_id IN ( "
    query += "      SELECT marker_id "
    query += "      FROM markers "
    query += "      WHERE site_id = %s) " %site_id
    query += "    ) AS site_hist "
    query += "  INNER JOIN "
    query += "    marker_names "
    query += "  USING(history_id) "
    query += "  ) AS names "
    query += "USING (marker_id) "
    data_source_df = pd.DataFrame(list(dbio.read(query)),
                                  columns=['marker_name'])
    data_source = ':' + ','.join(set(data_source_df['marker_name']))
    
    return data_source

def alert_details(site_id, trigger_source, ts_last_retrigger):
    
    start_monitor = monitoring_start(site_id, ts_last_retrigger)
    
    if trigger_source == 'rainfall':
        data_source = rainfall_details(site_id, start_monitor, ts_last_retrigger)
        
    elif trigger_source == 'subsurface':
        data_source = subsurface_details(site_id, start_monitor, ts_last_retrigger)
        
    elif trigger_source == 'surficial':
        data_source = surficial_details(site_id, start_monitor, ts_last_retrigger)
    else:  
        data_source = ''
    
    return data_source

def send_alert_message():
    # check due alert messages
    # ts_due = dt.today()
    # query = ("select alert_id, alert_msg from sms_alerts where alert_status is"
    #     " null and ts_set <= '%s'") % (ts_due.strftime("%Y-%m-%d %H:%M:%S"))

    # alertmsg = dbio.read(query,'send_alert_message')
    alert_msgs = check_alerts()

    contacts = get_alert_staff_numbers()

    if len(alert_msgs) == 0:
        print 'No alertmsg set for sending'
        return

    for (stat_id, site_id, site_code, trigger_source, alert_symbol, 
        ts_last_retrigger) in alert_msgs:
        tlr_str = ts_last_retrigger.strftime("%Y-%m-%d %H:%M:%S")
        message = ("SANDBOX:\n"
            "As of %s\n"
            "Alert ID %d:\n"
            "%s:%s:%s") % (tlr_str, stat_id, site_code,
                          alert_symbol, trigger_source)
        
        message += alert_details(site_id, trigger_source, ts_last_retrigger)
            
        message += "\n\nText\nSandbox ACK <alert_id> <validity> <remarks>"

        # send to alert staff        
        recipients_list = ""
        for mobile_id, sim_num, gsm_id in contacts:
            recipients_list += "%s," % (sim_num)
        recipients_list = recipients_list[:-1]
        smstables.write_outbox(message=message, recipients=recipients_list,
            gsm_id=gsm_id, table='users')
        
        # # set alert to 15 mins later
        ts_due = dt.now() + td(seconds=60*15)
        query = ("update alert_status set ts_set = '%s' where "
            "stat_id = %s") % (ts_due.strftime("%Y-%m-%d %H:%M:%S"),
            stat_id)

        dbio.write(query, 'checkalertmsg')

def check_alerts():
    ts_now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    query = ("SELECT stat_id, site_id, site_code, trigger_source, "
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

    alert_msgs = dbio.read(query,'check_alerts')

    print "alert messages:", alert_msgs

    return alert_msgs

def get_name_of_staff(number):
    query =  ("select t1.user_id, t2.nickname from user_mobile t1 inner join users t2 on "
        "t1.user_id = t2.user_id where t1.sim_num = '%s';") % (number)

    # print query

    name = dbio.read(query,'customquery')[0]

    return name

def process_ack_to_alert(sms):


    try:
        stat_id = re.search("(?<=K )\d+(?= )", sms.msg,re.IGNORECASE).group(0)
    except IndexError:
        errmsg = "Error in parsing alert id. Please try again"
        # smstables.write_outbox(errmsg,sms.sim_num)
        return False

    user_id, nickname = get_name_of_staff(sms.sim_num)
    print user_id, nickname, sms.msg
    if re.search("server",nickname.lower()):
        try:
            nickname = re.search("(?<=-).+(?= from)", sms.msg).group(0)
        except AttributeError:
            print "Error in processing nickname"
    # else:
    #     name = nickname

    try:
        remarks = re.search("(?<=\d ).+(?=($|\r|\n))",sms.msg, 
            re.IGNORECASE).group(0)
    except AttributeError:
        errmsg = "Please put in your remarks."
        smstables.write_outbox(message = errmsg, recipients = sms.sim_num,
            gsm_id=4,table='users')
        # write_outbox_dyna(errmsg, sms.sim_num)
        return True

    try:
        alert_status = re.search("(in)*valid(ating)*", remarks,
            re.IGNORECASE).group(0)
        remarks = remarks.replace(alert_status,"").strip()
    except AttributeError:
        errmsg = ("Please put in the alert status validity."
            " i.e (VALID, INVALID, VALIDATING)")
        smstables.write_outbox(message = errmsg, recipients = sms.sim_num,
            gsm_id=4, table='users')
        # write_outbox_dyna(errmsg, sms.sim_num)
        return True

    alert_status_dict = {"validating": 0, "valid": 1, "invalid": -1}

    query = ("update alert_status set user_id = %d, alert_status = %d, "
        "ts_ack = '%s', remarks = '%s' where stat_id = %s") % (user_id,
        alert_status_dict[alert_status.lower()], sms.ts, remarks, stat_id)
    # print query
    dbio.write(query,process_ack_to_alert)

    contacts = get_alert_staff_numbers()
    message = ("SANDBOX (test ack):\nAlert ID %s ACK by %s on %s\nStatus: %s\n"
        "Remarks: %s") % (stat_id, nickname, sms.ts, alert_status, remarks)
    
    tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    recipients_list = ""
    for mobile_id, sim_num, gsm_id in contacts:
        recipients_list += "%s," % (sim_num)
    recipents_list = recipients_list[:-1]
    smstables.write_outbox(message = message, recipients = recipients_list,
        gsm_id=gsm_id, table='users')
    # write_outbox_dyna(message,sim_num)
    # print message, sim_num

    return True

def update_shift_tags():
    # remove tags to old shifts
    today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    print 'Updating shift tags for', today

    query = ("update senslopedb.dewslcontacts set grouptags = "
        "replace(grouptags,',alert-mon','') where grouptags like '%alert-mon%'")
    dbio.write(query, 'update_shift_tags')

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
    dbio.write(query, 'update_shift_tags')

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
