import pandas as pd
import MySQLdb, time 
from sqlalchemy import create_engine
import  sqlalchemy.exc
import memcache
from sqlalchemy import MetaData
from sqlalchemy import Table



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

def df_engine(host='local'):
    dbc = dbInstance(host)
    engine = create_engine('mysql+pymysql://'+dbc.user+':'
        +dbc.password+'@'+dbc.host+':3306/'+dbc.name)
    return engine

def df_write(dataframe,host='local'):
    engine = df_engine(host)
    df = dataframe.data
    df = df.drop_duplicates(subset=None, keep='first',
     inplace=False)
    df = df.reset_index()
    df_list = str(df.values.tolist())[:-1][1:]
    df_list =df_list.replace("]",")").replace("[","(")
    df_header = str(list(df))[:-1][1:].replace("\'","")
    df_keys =[];
    for value in list(df):
        df_keys.append(value +" = VALUES("+value+")")
    df_keys = str(df_keys)[:-1][1:]
    df_keys =df_keys.replace("]",")")
    df_keys =df_keys.replace("[", "(").replace("\'", "")
    query = "insert into %s (%s) values %s" % (dataframe.name,
        df_header,df_list)
    query += " on DUPLICATE key update  %s " % (df_keys)
    try:
        write(query=query, 
            identifier='Insert dataFrame values')
    except IndexError:
        print "\n\n>> Error: Possible data type error"
    except ValueError:
        print ">> Value error detected"   
    except AttributeError:
        print ">> Value error in data pass"       

