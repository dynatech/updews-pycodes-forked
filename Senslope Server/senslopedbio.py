import ConfigParser, MySQLdb, time, sys
from datetime import datetime as dt
import cfgfileio as cfg

# cfg = ConfigParser.ConfigParser()
# cfg.read(sys.path[0] + "/senslope-server-config.txt")

class dbInstance:
    def __init__(self,name,host,user,password):
       self.name = name
       self.host = host
       self.user = user
       self.password = password

c = cfg.config()

localdbinstance = dbInstance(c.localdb.name,c.localdb.host,c.localdb.user,c.localdb.pwd)
gsmdbinstance = dbInstance(c.gsmdb.name,c.gsmdb.host,c.gsmdb.user,c.gsmdb.pwd)

# def SenslopeDBConnect():
# Definition: Connect to senslopedb in mysql
def SenslopeDBConnect(instance):
    if instance.upper() == 'LOCAL':
        dbc = localdbinstance
    else:
        dbc = gsmdbinstance
    while True:
        try:
            db = MySQLdb.connect(host = dbc.host, user = dbc.user, passwd = dbc.password, db = dbc.name)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
    	# except IndexError:
            print '6.',
            time.sleep(2)
            
def createTable(table_name, type):
    db, cur = SenslopeDBConnect('local')
    # cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    # cur.execute("USE %s"%Namedb)
    table_name = table_name.lower()
    
    if type == "sensor v1":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, xvalue int, yvalue int, zvalue int, mvalue int, PRIMARY KEY (timestamp, id))" %table_name)
    elif type == "sensor v2":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, msgid smallint, xvalue int, yvalue int, zvalue int, batt double, PRIMARY KEY (timestamp, id, msgid))" %table_name)
    elif type == "weather":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, name char(4), temp double,wspd int, wdir int,rain double,batt double, csq int, PRIMARY KEY (timestamp, name))" %table_name)
    elif type == "arqweather":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, name char(6), r15m double, r24h double, batv1 double, batv2 double, cur double, boostv1 double, boostv2 double, charge int, csq int, temp double, hum double, flashp int, PRIMARY KEY (timestamp, name))" %table_name)
    elif type == "piezo":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, name char(7), msgid int , freq double, temp double, PRIMARY KEY (timestamp, name))"%table_name)
    elif type == "stats":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, site char(4), voltage double, chan int, att int, retVal int, msgs int, sim int, csq int, sd int, PRIMARY KEY (timestamp, site))" %table_name)
    elif type == "soms":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, msgid smallint, mval1 int, mval2 int, PRIMARY KEY (timestamp, id, msgid))" %table_name)
    elif type == "runtime":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, script_name char(7), status char(10), PRIMARY KEY (timestamp, script_name))" %table_name)
    elif type == "gndmeas":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, meas_type char(10), site_id char (3), observer_name char(100), crack_id char(1), meas float(6,2), weather char(20), PRIMARY KEY (timestamp, meas_type, site_id, crack_id))" %table_name)
    elif type == "coordrssi":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, site_name char(5), router_name char(7), rssi_val smallint(20), PRIMARY KEY (timestamp, site_name, router_name))" %table_name)
    elif type == "smsinbox":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(sms_id int unsigned not null auto_increment, timestamp datetime, sim_num varchar(20), sms_msg varchar(255), read_status varchar(20), PRIMARY KEY (sms_id))" %table_name)
    elif type == "smsoutbox":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(sms_id int unsigned not null auto_increment, timestamp_written datetime, timestamp_sent datetime, recepients varchar(255), sms_msg varchar(255), send_status varchar(20), PRIMARY KEY (sms_id))" %table_name)
    elif type == "earthquake":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(e_id int unsigned not null auto_increment, timestamp datetime, mag float(6,2), depth float (6,2), lat float(6,2), longi float(6,2), dist tinyint unsigned, heading varchar(5), municipality varchar(50), province varchar(50), issuer varchar(10), PRIMARY KEY (e_id,timestamp))" %table_name)
    elif type == "servermonsched":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(p_id int unsigned not null auto_increment, date date, nickname varchar(20), primary key (p_id))" %table_name)
    elif type == "monshiftsched":
        cur.execute("CREATE TABLE IF NOT EXISTS %s(s_id int unsigned not null auto_increment, timestamp datetime, iompmt varchar(20), iompct varchar(20), oomps varchar(20), oompmt varchar(20), oompct varchar(20), primary key (s_id,timestamp))" %table_name)
    else:
        raise ValueError("ERROR: No option for creating table " + type)
   
        
    db.close()
    
def setReadStatus(read_status,sms_id_list):
    db, cur = SenslopeDBConnect('gsm')
    
    if len(sms_id_list) <= 0:
        return

    query = "update %s.smsinbox set read_status = '%s' where sms_id in (%s) " % (gsmdbinstance.name, read_status, str(sms_id_list)[1:-1].replace("L",""))
    commitToDb(query,"setReadStatus", instance='GSM')
    
def setSendStatus(send_status,sms_id_list):
    db, cur = SenslopeDBConnect('gsm')
    
    if len(sms_id_list) <= 0:
        return
        
    now = dt.today().strftime("%Y-%m-%d %H:%M:%S")

    query = "update %s.smsoutbox set send_status = '%s', timestamp_written ='%s' where sms_id in (%s) " % (gsmdbinstance.name, send_status, now, str(sms_id_list)[1:-1].replace("L",""))
    commitToDb(query,"setSendStatus", instance='GSM')
    
def getAllSmsFromDb(read_status):
    db, cur = SenslopeDBConnect('gsm')
    
    while True:
        try:
            query = """select sms_id, timestamp, sim_num, sms_msg from %s.smsinbox
                where read_status = '%s' limit 200""" % (gsmdbinstance.name, read_status)
        
            a = cur.execute(query)
            out = []
            if a:
                out = cur.fetchall()
            return out

        except MySQLdb.OperationalError:
            print '9.',
            
def getAllOutboxSmsFromDb(send_status,limit=10):
    db, cur = SenslopeDBConnect('gsm')
    
    while True:
        try:
            query = """select sms_id, timestamp_written, recepients, sms_msg from %s.smsoutbox
                where send_status = '%s' limit %d""" % (gsmdbinstance.name, send_status,limit)
        
            # print query
            a = cur.execute(query)
            out = []
            if a:
                out = cur.fetchall()
                db.close()
            return out

        except MySQLdb.OperationalError:
            print '9.',

def commitToDb(query, identifier, instance='local'):
    db, cur = SenslopeDBConnect(instance)

    # print query
    
    try:
        retry = 0
        while True:
            try:
                a = cur.execute(query)
                # db.commit()
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
    # except:
        # print '>> Unexpected error in writing to database query', query, 'from', identifier
    finally:
        db.close()

def querydatabase(query, identifier, instance='local'):
    db, cur = SenslopeDBConnect(instance)
    a = ''
    try:
        a = cur.execute(query)
        # db.commit()
        if a:
            a = cur.fetchall()
        else:
            # print '>> Warning: Query has no result set', identifier
            a = None
    except MySQLdb.OperationalError:
        a =  None
    except KeyError:
        a = None
    finally:
        db.close()
        return a

def checkNumberIfExists(simnumber,table='community'):
    simnumber = simnumber[-10:]
    if table == 'community':
        query = """select lastname,firstname,sitename from %scontacts where
            number like "%s%s%s"; """ % (table,'%',simnumber,'%')
    elif table == 'dewsl':
        query = """select lastname,firstname from %scontacts where
            number like "%s%s%s"; """ % (table,'%',simnumber,'%')
    elif table == 'sensor': 
        query = """select name from site_column_sim_nums where
            sim_num like "%s%s%s"; """ % ('%',simnumber,'%')
    else:
        return None

    identity = querydatabase(query,'checknumber')

    return identity

