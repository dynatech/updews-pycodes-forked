import dynadb.db as dbio
import volatile.memory as mem
import time
import MySQLdb

def set_read_status(sms_id_list, read_status=0, table='', host='local'):
    
    if table == '':
        print "Error: Empty table"
        return

    if type(sms_id_list) is list:
        if len(sms_id_list) == 0:
            return
        else:
            where_clause = ("where inbox_id "
                "in (%s)") % (str(sms_id_list)[1:-1].replace("L",""))
    elif type(sms_id_list) is long:
        where_clause = "where inbox_id = %d" % (sms_id_list)
    else:
        print ">> Unknown type"        
    query = "update smsinbox_%s set read_status = %d %s" % (table, read_status, 
        where_clause)
    
    # print query
    dbio.write(query, "set_read_status", False, host)

def set_send_status(table, status_list, host):
    # print status_list
    query = ("insert into smsoutbox_%s_status (stat_id,send_status,ts_sent,outbox_id,gsm_id,mobile_id) "
        "values ") % (table[:-1])

    for stat_id,send_status,ts_sent,outbox_id,gsm_id,mobile_id in status_list:
        query += "(%d,%d,'%s',%d,%d,%d)," % (stat_id,send_status,ts_sent,outbox_id,gsm_id,mobile_id)

    query = query[:-1]
    query += (" on duplicate key update stat_id=values(stat_id), "
        "send_status=send_status+values(send_status),ts_sent=values(ts_sent)")

    # print query
    
    dbio.write(query, "set_send_status", False, host)

def get_all_sms_from_db(host='local', read_status=0, table='loggers', limit=200):
    db, cur = dbio.connect(host)

    if table in ['loggers','users']:
        tbl_contacts = '%s_mobile' % table[:-1]
    else:
        print 'Error: unknown table', table
        return
    
    while True:
        try:
            query = ("select inbox_id,ts_sms,sim_num,sms_msg from "
                "(select inbox_id,ts_sms,mobile_id,sms_msg from smsinbox_%s "
                "where read_status = %d order by inbox_id desc limit %d) as t1 "
                "inner join (select mobile_id, sim_num from %s) as t2 "
                "on t1.mobile_id = t2.mobile_id ") % (table, read_status, limit,
                tbl_contacts)
            # print query
        
            a = cur.execute(query)
            out = []
            if a:
                out = cur.fetchall()
            return out

        except MySQLdb.OperationalError:
            print '9.',
            time.sleep(20)

def get_all_outbox_sms_from_db(table='users',send_status=5,gsm_id=5,limit=10):
    """
        **Description:**
          -The function that get all outbox message that are not yet send.
         
        :param table: Table name and **Default** to **users** table .
        :param send_status:  **Default** to **5**.
        :param gsm_id: **Default** to **5**.
        :param limit: **Default** to **10**.
        :type table: str
        :type send_status: str
        :type gsm_id: int
        :type limit: int
        :returns: List of message
    """
    sc = mem.server_config()
    host = sc['resource']['smsdb']

    while True:
        try:
            db, cur = dbio.connect(host)
            query = ("select t1.stat_id,t1.mobile_id,t1.gsm_id,t1.outbox_id,t2.sms_msg from "
                "smsoutbox_%s_status as t1 "
                "inner join (select * from smsoutbox_%s) as t2 "
                "on t1.outbox_id = t2.outbox_id "
                "where t1.send_status < %d "
                "and t1.send_status >= 0 "
                "and t1.gsm_id = %d "
                "limit %d ") % (table[:-1],table,send_status,gsm_id,limit)
          
            a = cur.execute(query)
            out = []
            if a:
                out = cur.fetchall()
                db.close()
            return out

        except MySQLdb.OperationalError:
            print '10.',
            time.sleep(20)