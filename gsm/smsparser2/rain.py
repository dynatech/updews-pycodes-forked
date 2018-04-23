import sys,re
import pandas as pd
import smsclass
from datetime import datetime as dt
import dynadb.db as dynadb


def check_number_in_users(num):
    """
       - Checks if the number exists in the user_mobile table.
      
      :param num: Cellphone number.
      :type num: int
      :returns: **query output** - Data output from the query.  

     
    """
    query = "select user_id from user_mobile where sim_num = '%s'" % (num)
    query = read(query=query, identifier='check if number user exists', 
        instance='local')
    return query

def check_logger_model(logger_name):
    """
       - Checks if the logger name exists in the loggers table.
      
      :param logger_name: Logger Name.
      :type logger_name: str
      :returns: **query output** - Data output from the query.   

     
    """
    query = ("SELECT model_id FROM loggers where "
        "logger_name = '%s'") % logger_name

    query = dynadb.read(query,'check_logger_model')[0][0]
    return query

def check_name_of_number(number):
    """
       - Checks if the name of the number exists in the loggers table.
      
      :param number: Cellphone number.
      :type number: int
      :returns: **query output** - Data output from the query.  

     
    """
    query = ("select logger_name from loggers where "
                "logger_id = (select logger_id from logger_mobile "
                "where sim_num = '%s' order by date_activated desc limit 1)" 
                % (number)
                )
    query = dynadb.read(query,'check_name_of_number')[0][0]
    return query

def rain_arq(sms):
    """
       - Process the sms message that fits for rain arq data.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **Dataframe**  - Retuen Dataframe structure output and if not return False for fail to parse message.

    """    
    #msg = message
    line = sms.msg
    sender = sms.sim_num

    print 'ARQ Weather data: ' + line

    line = re.sub("(?<=\+) (?=\+)","NULL",line)

    try:
        #table name
        linesplit = line.split('+')
       
        msgname = check_name_of_number(sender).lower()
        if msgname:
            print ">> Number registered as", msgname
            msgname_contact = msgname
        else:
            print ">> None type"
            return

            
        rain = int(linesplit[1])*0.5
        batv1 = linesplit[3]
        batv2 = linesplit[4]
        csq = linesplit[9]
        
        if csq=='':
            csq = 'NULL'
        temp = linesplit[10]
        hum = linesplit[11]
        flashp = linesplit[12]
        txtdatetime = dt.strptime(linesplit[13],
            '%y%m%d/%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

    except ValueError:    
        print '>> Error: Possible conversion mismatch ' + line
        return

    try:
        if csq != 'NULL':
            df_data = [{'ts':txtdatetime,'rain':rain,'temperature':temp,
            'humidity':hum,'battery1':batv1,'battery2':batv2,'csq':csq}]
        else:
            df_data = [{'ts':txtdatetime,'rain':rain,'temperature':temp,
            'humidity':hum,'battery1':batv1,'battery2':batv2}]

        df_data = pd.DataFrame(df_data).set_index('ts')
        df_data = smsclass.DataTable('rain_'+msgname,df_data)
        return df_data
    except ValueError:
        print '>> Error writing query string.', 
        return



def v3 (sms):
    """
       - Process the sms message that fits for v3 data rain data.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **Dataframe**  - Retuen Dataframe structure output and if not return False for fail to parse message.

    """    
    line = sms.msg
    sender = sms.sim_num
    
    #msg = message
    line = re.sub("[^A-Z0-9,\/:\.\-]","",line)

    print 'Weather data: ' + line
    
    if len(line.split(',')) > 9:
        line = re.sub(",(?=$)","",line)
    line = re.sub("(?<=,)(?=(,|$))","NULL",line)
    line = re.sub("(?<=,)NULL(?=,)","0.0",line)
    # line = re.sub("(?<=,).*$","NULL",line)
    print "line:", line

    try:
    
        logger_name = check_name_of_number(sender)
        logger_model = check_logger_model(logger_name)
        print logger_name,logger_model
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
        print line

        csq = line.split(",")[8]


    except IndexError, AttributeError:
        print '\n>> Error: Rain message format is not recognized'
        print line
        return False
    except ValueError:
        print '\n>> Error: One of the values not correct'
        print line
        return False
    except KeyboardInterrupt:
        print '\n>>Error: Weather message format unknown ' + line
        return False

    try:
        # query = ("INSERT INTO rain_%s (ts,rain,csq) "
        #     "VALUES ('%s',%s,%s)") % (msgtable.lower(),txtdatetime,rain,csq)
        # print query   
        if csq != 'NULL':
            df_data = [{'ts':txtdatetime,'rain':rain,'csq':csq}]
        else:
           df_data = [{'ts':txtdatetime,'rain':rain}]

        df_data = pd.DataFrame(df_data).set_index('ts')
        df_data = smsclass.DataTable('rain_'+msgtable.lower()
            ,df_data)
        return df_data         
    except:
        print '>> Error writing weather data to database. ' +  line
        return
    