""" Mirroring Data from dyna to sanbox and sandbox to dyna."""

import ConfigParser, MySQLdb, time, sys, argparse
from datetime import datetime as dt
import cfgfileio as cfg

# cfg = ConfigParser.ConfigParser()
# cfg.read(sys.path[0] + "/senslope-server-config.txt")
c = cfg.config()

class dbInstance:
    def __init__(self,host):
       self.name = c.db["name"]
       self.host = c.dbhost[host]
       self.user = c.db["user"]
       self.password = c.db["password"]

# def db_connect():
# Definition: Connect to senslopedb in mysql
def db_connect(host='local'):
    dbc = dbInstance(host)

    while True:
        try:
            db = MySQLdb.connect(host = dbc.host, user = dbc.user, 
                passwd = dbc.password, db = dbc.name)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
        # except IndexError:
            print '6.',
            time.sleep(2)
            
def set_read_status(sms_id_list,read_status=0,table='',instance='local'):
    
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
    commit_to_db(query, "set_read_status", False, instance)
    
def set_send_status(table, status_list, instance):
    # print status_list
    query = ("insert into smsoutbox_%s_status (stat_id,send_status,ts_sent,outbox_id,gsm_id,mobile_id) "
        "values ") % (table[:-1])

    for stat_id,send_status,ts_sent,outbox_id,gsm_id,mobile_id in status_list:
        query += "(%d,%d,'%s',%d,%d,%d)," % (stat_id,send_status,ts_sent,outbox_id,gsm_id,mobile_id)

    query = query[:-1]
    query += (" on duplicate key update stat_id=values(stat_id), "
        "send_status=send_status+values(send_status),ts_sent=values(ts_sent)")

    # print query
    
    commit_to_db(query, "set_send_status", False, instance)
    
    
def get_all_sms_from_db(host='local',read_status=0,table='loggers',limit=200):
    db, cur = db_connect(host)

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

    while True:
        try:
            db, cur = db_connect()
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

def get_logger_names(logger_type="all",instance="local"):
    db, cur = db_connect(instance)

    if logger_type == 'tilt':
        query = "SELECT distinct(tsm_name) FROM tsm_sensors;"
    elif logger_type in ['soms','piezo']:
        query = ("SELECT `logger_name` from `loggers` where `model_id` in "
            "(SELECT `model_id` FROM `logger_models` where `has_%s`=1) "
            "and `logger_name` is not null") % (logger_type)
    elif logger_type == 'rain':
        query = ("SELECT `logger_name` from `loggers` where `model_id` in "
            "(SELECT `model_id` FROM `logger_models` where `has_%s`=1 "
            "or `logger_type`='arq') and `logger_name` is not null")
    else:
        print 'Error: No info for logger type', logger_type
        return

    # print query 
    result_set = query_database(query,"createSensorColumnTables",instance)

    # print result_set
    names = []
    print names
    for row in result_set:
        names.append(row[0])

    return names

def create_logger_tables(logger_type='all',instance="local"):
    db, cur = db_connect(instance)

    logger_names = get_logger_names(logger_type,instance)

    query = ''

    if logger_type == 'soms':
        for n in logger_names:
            query += ("CREATE TABLE IF NOT EXISTS `soms_%s` ("
                "`data_id` INT NOT NULL AUTO_INCREMENT,"
                "`ts` TIMESTAMP NULL,"
                "`node_id` TINYINT NULL,"
                "`type_num` SMALLINT UNSIGNED NULL,"
                "`mval1` SMALLINT UNSIGNED NULL,"
                "`mval2` SMALLINT UNSIGNED NULL,"
                "PRIMARY KEY (`data_id`),"
                "UNIQUE INDEX `unique1` (`ts` ASC, `node_id` ASC, "
                "`type_num` ASC)) "
                "ENGINE = InnoDB;\n\n") % (n)

    elif logger_type == 'piezo':
        for n in logger_names:
            query += ("CREATE TABLE IF NOT EXISTS `piezo_%s` ("
                "`data_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                "`ts` TIMESTAMP NULL DEFAULT NULL,"
                "`frequency_shift` DECIMAL(6,2) UNSIGNED NULL DEFAULT NULL,"
                "`temperature` FLOAT NULL DEFAULT NULL,"
                "PRIMARY KEY (`data_id`),"
                "UNIQUE INDEX `unique1` (`ts` ASC)) "
                "ENGINE = InnoDB;\n\n") % (n)

    elif logger_type == 'rain':
        for n in logger_names:
            query +=  ("CREATE TABLE IF NOT EXISTS `rain_%s` ("
                "`data_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                "`ts` TIMESTAMP NULL,"
                "`rain` FLOAT NULL DEFAULT NULL,"
                "`temperature` FLOAT NULL DEFAULT NULL,"
                "`humidity` FLOAT NULL DEFAULT NULL,"
                "`battery1` FLOAT NULL DEFAULT NULL,"
                "`battery2` FLOAT NULL DEFAULT NULL,"
                "`csq` TINYINT(3) NULL DEFAULT NULL,"
                "PRIMARY KEY (`data_id`),"
                "UNIQUE INDEX `unique1` (`ts` ASC))"
                "ENGINE = InnoDB;\n\n") % (n)
    
    elif logger_type == 'tilt':
        for n in logger_names:
            query += ("CREATE TABLE IF NOT EXISTS `tilt_%s` ("
                "`data_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
                "`ts` TIMESTAMP NULL DEFAULT NULL,"
                "`node_id` TINYINT(3) UNSIGNED NULL DEFAULT NULL,"
                "`type_num` TINYINT(3) UNSIGNED NULL DEFAULT NULL,"
                "`xval` SMALLINT(6) NULL DEFAULT NULL,"
                "`yval` SMALLINT(6) NULL DEFAULT NULL,"
                "`zval` SMALLINT(6) NULL DEFAULT NULL,"
                "`batt` FLOAT(11) NULL DEFAULT NULL,"
                "PRIMARY KEY (`data_id`),"
                "UNIQUE INDEX `unique1` (`ts` ASC, `node_id` ASC, "
                "`type_num` ASC))"
                "ENGINE = InnoDB "
                "DEFAULT CHARACTER SET = utf8;\n\n") % (n)

    else:
        print 'Error: No create info for logger type', logger_type
        return

    # print query

    cur.execute(query)
    db.close()

def commit_to_db(query, identifier, last_insert=False, instance='local'):
    db, cur = db_connect(instance)

    # print query
    b=''
    
    try:
        retry = 0
        while True:
            try:
                a = cur.execute(query)
                # db.commit()
                b = ''
                if last_insert:
                    b = cur.execute('select last_insert_id()')
                    b = cur.fetchall()

                if a:
                    db.commit()
                    break
                else:
                    # print '>> Warning: Query has no result set', identifier
                    db.commit()
                    time.sleep(0.1)
                    break
            # except MySQLdb.OperationalError:
            except IndexError:
                print '5.',
                #time.sleep(2)
                if retry > 10:
                    break
                else:
                    retry += 1
                    time.sleep(2)
    except KeyError:
        print '>> Error: Writing to database', identifier
    except MySQLdb.IntegrityError:
        print '>> Warning: Duplicate entry detected', identifier
    db.close()
    return b

def query_database(query, identifier='', instance='local'):
    db, cur = db_connect(instance)
    a = ''

    # print query, identifier
    try:
        a = cur.execute(query)
        # db.commit()
        # if a:
        #     a = cur.fetchall()
        # else:
        #     # print '>> Warning: Query has no result set', identifier
        #     a = None
        try:
            a = cur.fetchall()
            return a
        except ValueError:
            return None
    except MySQLdb.OperationalError:
        a =  None
    except KeyError:
        a = None
    # except MySQLdb.ProgrammingError:
    #     print 'ERROR: Check sql query'
    #     a = None
    # finally:
    #     db.close()
    #     return a

def check_number_if_exists(simnumber,table='community'):
    simnumber = simnumber[-10:]
    if table == 'community':
        query = ("select lastname,firstname,sitename from %scontacts where "
            "number like '%s%s%s';") % (table,'%',simnumber,'%')
    elif table == 'dewsl':
        query = ("select lastname,firstname from %scontacts where"
            "number like '%s%s%s';") % (table,'%',simnumber,'%')
    elif table == 'sensor': 
        query = ("select name from site_column_sim_nums where"
            "sim_num like '%s%s%s';") % ('%',simnumber,'%')
    else:
        return None

    identity = query_database(query,'checknumber')

    return identity

def get_arguments():
    desc_str = "senslopedbio\n senslopedbio [-options]"
    parser = argparse.ArgumentParser(description = desc_str)
    parser.add_argument("-t", "--test", help = "run test function",
        action = "store_true")
    parser.add_argument("-c", "--create_tables", help = "run test function")    
    
    try:
        args = parser.parse_args()
        return args
    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()

def main():
    args = get_arguments()

    if args.test:
        test()
        sys.exit()

    if args.create_tables:
        tables_to_create = args.create_tables.split(",")
        for t in tables_to_create:
            try:
                create_logger_tables(t,'sandbox')
            except:
                print 'Warning: No table info for', t
    else:
        print 'No tables to create'

    return

# for test codes    
def test():
    args = get_arguments()

    if args.create_tables:
        print args.create_tables
    else:
        print 'Will not create tables'

    # dropTsmSensorTables("backup")
    # createSomsTables("backup")
    # createTTables("backup")
    # createTsmSensorTables("backup")
    # createTsmSensorTables("backup")
    # create_logger_tables("piezo","sandbox")

    # print 
    return

if __name__ == "__main__":
    main()
