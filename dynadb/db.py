import inspect
import memcache
import MySQLdb
import pandas.io.sql as psql
import sqlalchemy.exc
from sqlalchemy import create_engine
import sys
import time 


mc = memcache.Client(['127.0.0.1:11211'],debug=0)


def get_connector(dbc='',conn_type=''):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 

    if type(dbc) is dict:
        dbc = [dbc]

    for list_dbc in dbc:
        if conn_type == 1:
           dbc_output = get_msqldb_connect(list_dbc)
        elif conn_type == 0:
           dbc_output = get_create_engine(list_dbc)

        if dbc_output:
            return dbc_output
        else:
            print ('Connection Fail')
            continue
    return False
    

def get_msqldb_connect(dbc):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    try:
        db = MySQLdb.connect(dbc['host'], dbc['user'], 
            dbc['password'], dbc['schema'])
        cur = db.cursor()
        return db, cur
    except TypeError:
        print ('Error Connection Value')
        return False
    except MySQLdb.OperationalError:
        print ('6.',)
        time.sleep(2)
        return False
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False


def get_create_engine(dbc):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    try:
        engine = create_engine('mysql+pymysql://' + 
            dbc['user'] + ':'+ dbc['password'] + '@' + 
            dbc['host'] +':3306/' + dbc['schema'])
        return engine
    except sqlalchemy.exc.OperationalError:
        print (">> Error in connetion")
        return False


def get_connection_info(connection_name):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    try:
        conn_dict = mc.get('DICT_DB_CONNECTIONS')
        dbc = conn_dict[connection_name]
        return dbc
    except TypeError:
        print ('Unknown DICT_DB_CONNECTIONS ')
        return None


def connect(host='', connection='', resource='' , conn_type=1):   
    """
    - Creating the ``MySQLdb.connect`` and ``create_engine``connetion for the database.

    Args:
        host (str): Hostname.

    Returns:
        Returns the ``MySQLdb.connect()`` as db and ``db.cursor()`` as cur 
        connection to the host.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    # dbc = database connection
    #sc = server_config
    dbc = None
    if connection:
        dbc = get_connection_info(connection)
    elif resource:
        dbc = None 
        try:
            sc = mc.get('SERVER_CONFIG')
            resource_dict = sc['resource_connection'][resource]
            resource_dict = resource_dict.split(",")
            dbc = []
            for connection in resource_dict:
                dbc.append(get_connection_info(connection))
        except KeyError:
            print ('Unknown Resource ' + resource)
    elif host:
        dbc = None 
        try:
            sc = mc.get('SERVER_CONFIG')
            dbc = dict()
            dbc['host'] = sc['hosts'][host]
            dbc['user'] = sc['db']['user'] 
            dbc['password'] = sc['db']['password']
            dbc['schema'] = sc['db']['name'] 
            
        except KeyError:
            print ("Unknown Host " + host)
    
    if dbc:
        return get_connector(dbc=dbc, conn_type=conn_type)
    else:    
        print ('Cannot Connect to Database Connection')
        return False


def write(query ='', identifier = '', last_insert=False, 
    host='local', connection='', resource=''):
    """
    - The process of writing to the database by a query statement.

    Args:
        query (str): Query statement.
        identifier (str): Identifier statement for the query.
        Last_insert (str): Select the last insert. Defaults to False.
        host (str): Hostname. Defaults to local.
    
    Raises:
        IndexError: Error in retry index.
        KeyError: Error on writing to database.
        MySQLdb.IntegrityError: If duplicate entry detected.

    """ 
    ret_val = None
    db, cur = connect(host=host, connection=connection, 
        resource=resource)

    try:
        db.commit()
        if last_insert:
            b = cur.execute('select last_insert_id()')
            b = cur.fetchall()
            ret_val = b

    except IndexError:
        print ("IndexError on ",)
        print (str(inspect.stack()[1][3]))
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print (">> MySQL error/warning: %s" % e)
        print ("Last calls:") 
        for i in range(1,6):
            try:
                print ("%s," % str(inspect.stack()[i][3]),)
            except IndexError:
                continue
        print ("\n")

    finally:
        db.close()
        return ret_val


def read(query='', identifier='', host='local', 
    connection='', resource=''):
    """
    - The process of reading the output from the query statement.

    Args:
        query (str): Query statement.
        identifier (str): Identifier statement for the query.
        host (str): Hostname. Defaults to local.

    Returns:
      tuple: Returns the query output and fetch by a ``cur.fetchall()``.

    Raises:
        KeyError: Key interruption.
        MySQLdb.OperationalError: Error in database connection.
        ValueError: Error in execution of the query.

    Example Output::
            
        >> print read(query='SELECT * FROM senslopedb.loggers limit 3', identifier='select loggers')
        ((1, 1, 'agbsb', datetime.date(2015, 8, 31), None, Decimal('11.280820'), Decimal('122.831300'), 3), 
        (2, 1, 'agbta', datetime.date(2015, 8, 31), None, Decimal('11.281370'), Decimal('122.831100'), 6), 
        (3, 2, 'bakg', datetime.date(2016, 8, 9), None, Decimal('16.789631'), Decimal('120.660903'), 31))

    """ 
    ret_val = None
    caller_func = str(inspect.stack()[1][3])
    db, cur = connect(host=host, connection=connection, 
        resource=resource)

    try:
        a = cur.execute(query)
        try:
            a = cur.fetchall()
            ret_val = a
        except ValueError:
            ret_val = None
    except MySQLdb.OperationalError:
        print ("MySQLdb.OperationalError on ",)
        print (caller_func)
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print (">> MySQL Error or warning: ", )
        print (e, "from", )
        print (caller_func)
    except KeyError:
        print ("KeyError on ",)
        print (caller_func)
    finally:
        db.close()
        return ret_val


def df_write(data_table, host='local', last_insert=False , 
    connection='' , resource=''):
    """
    - The process of writing data frame data to a database.

    Args:
        data_table (obj): DataTable class object from smsclass.py.
        host (str): Hostname. Defaults to local.

    Raises:
        IndexError: Possible data type error.
        ValueError: Value error detected.
        AttributeError: Value error in data pass.


    """
    df = data_table.data
    df = df.drop_duplicates(subset=None, keep='first', inplace=False)
    value_list = str(df.values.tolist())[:-1][1:]
    value_list = value_list.replace("]",")").replace("[","(")
    value_list = value_list.replace("nan", "NULL")

    column_name_str = ""
    for name in list(df):
        column_name_str += name.encode('ascii','ignore') + ","
    column_name_str = column_name_str[:-1]

    duplicate_value_str = ", ".join(["%s = VALUES(%s)" % (name, name) 
        for name in list(df)]) 
    query = 'insert into %s (%s) values %s' % (data_table.name,
        column_name_str, value_list)
    query += ' on DUPLICATE key update  %s ' % (duplicate_value_str)

    try:
        last_insert_id = write(query=query, 
            identifier='Insert dataFrame values', 
            last_insert=last_insert,
            host=host,
            connection=connection, 
            resource=resource)

        return last_insert_id

    except IndexError:
        print ('\n\n>> Error: Possible data type error')
    except ValueError:
        print ('>> Value error detected'   )
    except AttributeError:
        print (">> Value error in data pass"       )


def df_read(query='', host='local', connection='', resource=''):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    db = connect(host=host, connection=connection, 
        resource=resource, conn_type=0)
    ret_val = None
    try:
        df = psql.read_sql_query(query, db)
        ret_val = df
    except KeyboardInterrupt:
        print ('Exception detected in accessing database')
        sys.exit()
    except psql.DatabaseError:
        print ('Error getting query %s' % (query))
        ret_val = None
    finally:
        return ret_val
