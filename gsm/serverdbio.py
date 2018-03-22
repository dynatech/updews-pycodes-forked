""" Mirroring Data from dyna to sanbox and sandbox to dyna."""

import ConfigParser, MySQLdb, time, sys, argparse
from datetime import datetime as dt
import memcache
mc = memcache.Client(['127.0.0.1:11211'],debug=0)

class dbInstance:
    def __init__(self,host):
        sc = mc.get('server_config')
        self.name = sc['db']['name']
        self.host = sc['hosts'][host]
        self.user = sc['db']['user']
        self.password = sc['db']['password']
       # self.name = c.db["name"]
       # self.host = c.dbhost[host]
       # self.user = c.db["user"]
       # self.password = c.db["password"]

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
