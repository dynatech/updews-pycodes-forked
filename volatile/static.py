from datetime import datetime as dt
import MySQLdb
import os
import pandas.io.sql as psql
import sqlalchemy
import sys
import warnings

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as dbio
import volatile.memory as memory


class VariableInfo:
    def __init__(self, info):
        """
        - Description.

        Args:
            Args (str): Args.

        Returns:
            Returns.

        Raises:
            MySQLdb.OperationalError: Error in database connection.

        """    
        self.name = str(info[0])
        self.query = str(info[1])
        self.type = str(info[2])
        self.index_id = str(info[3])
        self.resource = str(info[4])
        
        
def dict_format(query_string, variable_info):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    query_output = dbio.read(query=query_string,resource=variable_info.resource)
    if query_output:
        dict_output = {a: b for a, b in query_output}
        return dict_output
    else:
        return False


def set_static_variable(name=""):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    query = ("Select name, query, data_type, ts_updated, resource from "
        "static_variables")
    date = dt.now()
    date = date.strftime('%Y-%m-%d %H:%M:%S')
    if name != "":
        query += " where name = '%s'" % (name)

    try:
        variables = dbio.read(query=query, resource='common_data')
    except MySQLdb.ProgrammingError:
        print (">> static_variables table does not exist on host")
        return

    if not variables:
        print ("Error getting static variable information")
        return False

    for data in variables:
        variable_info = VariableInfo(data)

        if variable_info.type == 'data_frame':
            static_output = dbio.df_read(query=variable_info.query,
                resource=variable_info.resource)

        elif variable_info.type == 'dict':
            static_output = dict_format(variable_info.query, 
                variable_info)

        else:
            static_output = dbio.read(query=variable_info.query,
                resource=variable_info.resource)
            
        if static_output is None: 
            warnings.warn('Query error: ' + variable_info.name, stacklevel=2)
        else:
            memory.set(variable_info.name, static_output)
            query_ts_update = ("UPDATE static_variables SET "
                " ts_updated ='%s' WHERE name ='%s'") % (date, 
                    variable_info.name)
            dbio.write(query=query_ts_update, resource='common_data')
            print (variable_info.name, "success")


def get_db_dataframe(query, host='local'):
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
        df = dbio.df_read(query, host=host)
        return df
    except KeyboardInterrupt:
        print ("Exception detected in accessing database")
        sys.exit()
    except psql.DatabaseError:
        print ("Error getting query %s" % (query))
        return None


def set_mysql_tables(mc):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    tables = ['sites','tsm_sensors','loggers','accelerometers']

    print ('Setting dataframe tables to memory')
    for key in tables:
        print ("%s," % (key),)
        df = get_db_dataframe("select * from %s;" % key)

        if df is None:
            continue

        mc.set('df_'+key,df)

        # special configuration
        if key == 'sites':
            mc.set(key+'_dict',df.set_index('site_code').to_dict())

    print (' ... done')


def get_mobiles(table=None,host=None,reset_variables=False,resource=None):
    """
        **Description:**
          -The get mobile sim nums is a function that get the number of the loggers or users in the database.
         
        :param table: loggers or users table.
        :param host: host name of the number and  **Default** to **None**
        :type table: str
        :type host: str 
        :returns:  **mobile number** (*int*) - mobile number of user or logger
    """
    mc = memory.get_handle()

    # if host is None:
    #     raise ValueError("No host value given for mobile number")

    if not table:
        raise ValueError("No table definition")

    is_reset_variables = reset_variables
    
    if table == 'loggers':

        logger_mobile_sim_nums = mc.get('logger_mobile_sim_nums')
        if logger_mobile_sim_nums and not is_reset_variables:
            return logger_mobile_sim_nums

        print ("Force reset logger mobiles in memory")

        query = ("SELECT t1.mobile_id, t1.sim_num, t1.gsm_id "
            "FROM logger_mobile AS t1 "
            "LEFT OUTER JOIN logger_mobile AS t2 "
            "ON t1.sim_num = t2.sim_num "
            "AND (t1.date_activated < t2.date_activated "
            "OR (t1.date_activated = t2.date_activated "
            "AND t1.mobile_id < t2.mobile_id)) "
            "WHERE t2.sim_num IS NULL and t1.sim_num is not null")

        nums = dbio.read(query=query, identifier='get_mobile_sim_nums', 
            host=host, resource=resource)

        logger_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("logger_mobile_sim_nums", logger_mobile_sim_nums)

        logger_mobile_def_gsm_id = {mobile_id: gsm_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("logger_mobile_def_gsm_id", logger_mobile_def_gsm_id)

    elif table == 'users':

        user_mobile_sim_nums = mc.get('user_mobile_sim_nums')
        if user_mobile_sim_nums and not is_reset_variables:
            return user_mobile_sim_nums

        print ("Force reset user mobiles in memory")
        
        query = "select mobile_id, sim_num, gsm_id from mobile_numbers"

        nums = dbio.read(query=query, identifier='get_mobile_sim_nums', 
            host=host, resource=resource)

        user_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("user_mobile_sim_nums",user_mobile_sim_nums)

        user_mobile_def_gsm_id = {mobile_id: gsm_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("user_mobile_def_gsm_id", user_mobile_def_gsm_id)

    else:
        print ('Error: table', table)
        sys.exit()

    return nums


def get_surficial_markers(host = None, from_memory = True):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    mc = memory.get_handle()
    sc = memory.server_config()

    if from_memory:
        return mc.get("surficial_markers")

    if not host:
        print ("Host defaults to datadb")
        host = sc["resource"]["datadb"]

    query = ("select m2.marker_id, m3.marker_name, m4.site_id from "
        "(select max(history_id) as history_id, "
        "marker_id from marker_history as m1 "
        "group by m1.marker_id "
        ") as m2 "
        "inner join marker_names as m3 "
        "on m2.history_id = m3.history_id "
        "inner join markers as m4 "
        "on m2.marker_id = m4.marker_id ")

    engine = dbio.connect(resource="sensor_data",conn_type=0)
    surficial_markers = psql.read_sql_query(query, engine)
    mc.set("surficial_markers", surficial_markers)

    return surficial_markers


def get_surficial_parser_reply_messages():
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 
    query = "select * from surficial_parser_reply_messages"
    
    df = get_db_dataframe(query)
    df = df.set_index("msg_id")

    return df


def set_variables_old(reset_variables):
    """
    - Description.

    Args:
        Args (str): Args.

    Returns:
        Returns.

    Raises:
        MySQLdb.OperationalError: Error in database connection.

    """ 

    print (dt.today().strftime('%Y-%m-%d %H:%M:%S'))
    mc = memory.get_handle()

    print ("Reset alergenexec",)
    mc.set("alertgenexec", False)
    print ("done")

    print ("Set static tables to memory",)
    try:
        set_mysql_tables(mc)
    except KeyError:
        print (">> KeyError")
    print ("done")

    print ("Set mobile numbers to memory",)
#    mobiles_host = sc["resource"]["mobile_nums_db"]
    get_mobiles(table="loggers", reset_variables=reset_variables, resource="sms_data")
    get_mobiles(table="users", reset_variables=reset_variables, resource="sms_data")
    print ("done")

    try:
        print ("Set surficial_markers to memory", )
        get_surficial_markers(from_memory = False)
        print ("done")

        print ("Set surficial_parser_reply_messages",)
        df = get_surficial_parser_reply_messages()
        mc.set("surficial_parser_reply_messages", df)
        print ("done")
    except sqlalchemy.exc.ProgrammingError: 
        print (">> Error on getting surficial information. Skipping load")
