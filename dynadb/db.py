
import MySQLdb, time 
#import cfgfileio as cfg
import memcache
mc = memcache.Client(['127.0.0.1:11211'],debug=0)


#c = cfg.config()

class dbInstance:
    def __init__(self,host):
        sc = mc.get('server_config')
        self.name = sc['db']['name']
        self.host = sc['hosts'][host]
        self.user = sc['db']['user']
        self.password = sc['db']['password']
      

def connect(host='local'):
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


def write(query='', identifier='', last_insert=False, instance='local'):
    db, cur = connect(instance)

    b=''
    try:
        retry = 0
        while True:
            try:
                a = cur.execute(query)

                b = ''
                if last_insert:
                    b = cur.execute('select last_insert_id()')
                    b = cur.fetchall()

                if a:
                    db.commit()
                    break
                else:

                    db.commit()
                    time.sleep(0.1)
                    break

            except IndexError:
                print '5.',

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

def read(query='', identifier='', instance='local'):
    db, cur = connect(instance)
    a = ''
    
    try:
        a = cur.execute(query)
        a = None
        try:
            a = cur.fetchall()
            return a
        except ValueError:
            return None
    except MySQLdb.OperationalError:
        a =  None
    except KeyError:
        a = None
def update_sim_num_table(name,sim_num,date_activated):
    return
    db, cur = connect('local')
    
    query = ("INSERT IGNORE INTO site_column_sim_nums (name,sim_num, "
        "date_activated) VALUES ('%s','%s','%s')" % (name.upper(),
        sim_num, date_activated)
        )

    write(query, 'update_sim_num_table')