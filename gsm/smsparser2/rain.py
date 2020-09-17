from datetime import datetime as dt
import os
import pandas as pd
import re
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as dynadb
import smsclass
#------------------------------------------------------------------------------

def check_number_in_users(num):
    """
    - The process of querying the mobile number to check if the number exists.

    :param num: Instance hostname.
    :type num: str

    Returns:
        tuple: Query output for success and return False if fails.

    Example Output::

        >>> x = check_number_in_users('639263818956')
        ((2,),)

    """   
    query = "select user_id from user_mobile" 
    query += "where sim_num = '%s'" % (num)
    query = dynadb.read(query=query, identifier='checkifexists', host='local')
    if len(query) != 0:
        return query[0][0]
    else:
        return

def check_logger_model(logger_name):
    """
    - The process of querying the logger name  to check if the logger name exists.

    :param logger_name: Instance hostname.
    :type logger_name: str

    Returns:
        str: Query output for success and return False if fails.

    Example Output::

        >>> x = check_logger_model('agbta')
        6

    """  
    query = ("SELECT model_id FROM loggers where "
        "logger_name = '%s'") % logger_name

    query = dynadb.read(query,'check_logger_model', connection='common')
    if len(query) != 0:
        return query[0][0]
    else:
        return

def check_name_of_number(number):
    """
    - The process of querying the mobile number  to check the cellphone number logger name.

    :param number: Cellphone number.
    :type number: int

    Returns:
        str: Query output for success and return False if fails.

    Example Output::

        >>> x = check_name_of_number('639173082161')
        agbta

    """  
    conn = mem.get('DICT_DB_CONNECTIONS')
    query = ("select logger_name from {}.loggers where "
                "logger_id = (select logger_id from {}.logger_mobile "
                "where sim_num like '%{}' order by date_activated desc limit 1)".format(conn['common']['schema'],conn['gsm_pi']['schema'],number) 
                )
    query = dynadb.read(query,'check_name_of_number', resource='sms_analysis')
    if len(query) != 0:
        return query[0][0]
    else:
        return

def rain_arq(sms):
    """
    - The process of parsing data of Arq message of rain.

    :param sms: Dictionary of sms info.
    :type sms: obj

    Returns:
       DataFrame: Dataframe output for success parsing and return
       False if fails.

    Example Output::
        
                            battery1 battery2 csq humidity  rain temperature
        ts
        2018-04-26 13:30:58    4.143    4.158   9     69.8   0.0        30.0
       
    """       
    #msg = message
    line = sms.msg
    sender = sms.sim_num[-10:]

    print ('ARQ Weather data: ' + line)

    line = re.sub("(?<=\+) (?=\+)","NULL",line)

    #table name
    linesplit = line.split('+')
   
    msgname = check_name_of_number(sender)
    if msgname:
        msgname = msgname.lower()
        print (">> Number registered as", msgname)
#        msgname_contact = msgname
    else:
        raise ValueError("Number not registered {}".format(sender))

    try:
        rain = int(linesplit[1])*0.5
        batv1 = linesplit[3]
        batv2 = linesplit[4]
        csq = linesplit[9]
    except IndexError:
        raise ValueError("Incomplete data")
    
    if csq=='':
        csq = 'NULL'

    try:
        temp = linesplit[10]
        hum = linesplit[11]
#        flashp = linesplit[12]
    except IndexError:
        raise ValueError("Incomplete data")
    txtdatetime = dt.strptime(linesplit[13],
        '%y%m%d/%H%M%S').strftime('%Y-%m-%d %H:%M:%S')


    try:
        if csq != 'NULL' and csq != 'N/A':
            df_data = [{'ts':txtdatetime,'rain':rain,'temperature':temp,
            'humidity':hum,'battery1':batv1,'battery2':batv2,'csq':csq}]
        else:
            df_data = [{'ts':txtdatetime,'rain':rain,'temperature':temp,
            'humidity':hum,'battery1':batv1,'battery2':batv2}]

        df_data = pd.DataFrame(df_data)
        df_data = smsclass.DataTable('rain_'+msgname,df_data)
        return df_data
    except ValueError:
        print ('>> Error writing query string.', )
        return None



def v3(sms): 
    """
    - The process of parsing data of v3 message of rain.

    :param sms: Dictionary of sms info.
    :type sms: obj

    Returns:
       DataFrame: Dataframe output for success parsing and return
       False if fails.

    Example Output::


                            battery1 battery2 csq humidity  rain temperature
        ts
        2018-04-26 13:30:58    null    null   15     null   0        null
    """    
    line = sms.msg
    sender = sms.sim_num
    
    #msg = message
    line = re.sub("[^A-Z0-9,\/:\.\-]","",line)

    print ('Weather data: ' + line)
    
    if len(line.split(',')) > 9:
        line = re.sub(",(?=$)","",line)
    line = re.sub("(?<=,)(?=(,|$))","NULL",line)
    line = re.sub("(?<=,)NULL(?=,)","0.0",line)
    # line = re.sub("(?<=,).*$","NULL",line)
    print ("line:", line)

    try:
    
        logger_name = check_name_of_number(sender)
        logger_model = check_logger_model(logger_name)
        print (logger_name,logger_model)
        if logger_model in [23,24,25,26]:
            msgtable = logger_name
        else:
            msgtable = line.split(",")[0][:-1]+'G'
        # msgtable = check_name_of_number(sender)
        msgdatetime = re.search("\d{02}\/\d{02}\/\d{02},\d{02}:\d{02}:\d{02}",
            line).group(0)

        txtdatetime = dt.strptime(msgdatetime,'%m/%d/%y,%H:%M:%S')
        
        txtdatetime = txtdatetime.strftime('%Y-%m-%d %H:%M:%S')
        
        # data = items.group(3)
        rain = line.split(",")[6]
        print (line)

        csq = line.split(",")[8]


    except (IndexError, AttributeError) as e:
        print ('\n>> Error: Rain message format is not recognized')
        print (line)
        return False
    except ValueError:
        print ('\n>> Error: One of the values not correct')
        print (line)
        return False
    except KeyboardInterrupt:
        print ('\n>>Error: Weather message format unknown ' + line)
        return False

    try:
        # query = ("INSERT INTO rain_%s (ts,rain,csq) "
        #     "VALUES ('%s',%s,%s)") % (msgtable.lower(),txtdatetime,rain,csq)
        # print query   
        if csq != 'NULL':
            df_data = [{'ts':txtdatetime,'rain':rain,'csq':csq}]
        else:
           df_data = [{'ts':txtdatetime,'rain':rain}]

        df_data = pd.DataFrame(df_data)
        df_data = smsclass.DataTable('rain_'+msgtable.lower()
            ,df_data)
        return df_data         
    except:
        print ('>> Error writing weather data to database. ' +  line)
        return
    
