import re, sys
from datetime import datetime as dt
import subprocess, time
import pandas as pd
import memcache
mc = memcache.Client(['127.0.0.1:11211'],debug=0)
sc = mc.get('server_config')
import dynadb.db as dynadb

class SurficialParserError(Exception):
    pass

def parse_surficial_text(text):
    print '\n\n*******************************************************'  
    print text
      
    # clean the message
    cleanText = re.sub(" +"," ",text.upper())
    cleanText = re.sub("\.+",".",cleanText)
    cleanText = re.sub(";",":",cleanText)
    cleanText = re.sub("\n"," ",cleanText)
    cleanText = cleanText.strip()
    sms_list = re.split(" ",re.sub("[\W]"," ",cleanText))
      
    sms_date = ""
    sms_time = ""
    records = []
      
      # check measurement type
    if sms_list[0][0] == 'R':
        meas_type = "ROUTINE"
    else:
        meas_type = "EVENT"
        
    try:
        data_field = re.split(" ",cleanText,maxsplit=2)[2]
    except IndexError:
        raise SurficialParserError(sc["replymessages"]["failmeasen"])

    # double check site code
    site_code = sms_list[1].lower()
    # site_id = get_site_id(site_code)
    # if site_id is None:
    sites_dict = mc.get('sites_dict')
    site_code = adjust_site_code(site_code)
    site_id = sites_dict['site_id'][site_code]
    
        
    
    # try:
    date_str = get_date_from_sms(data_field)
    print "Date: " + date_str
    # except AttributeError:
    #     raise ValueError(sc["replymessages"]["faildateen"])
      
    # try:
    time_str = get_time_from_sms(data_field)
    print "Time: " + time_str
    # except AttributeError:
        # raise ValueError(c.reply.failtimeen)
      
    # get all the measurement pairs
    meas_pattern = "(?<= )[A-Z] *\d{1,3}\.*\d{0,2} *C*M"
    meas = re.findall(meas_pattern,data_field)
      # create records list
    if meas:
        pass
    else:
        raise SurficialParserError(sc["replymessages"]["failmeasen"])
      
      # get all the weather information
    print meas[-1], repr(data_field)
    # try:
    try:
        wrecord = re.search("(?<="+meas[-1]+" )[A-Z]+",data_field).group(0)
    except AttributeError:
        raise SurficialParserError(sc["replymessages"]["failweaen"])
    recisvalid = False
    for keyword in ["ARAW","ULAN","BAGYO","LIMLIM","AMBON","ULAP","SUN",
        "RAIN","CLOUD","DILIM","HAMOG","INIT"]:
        if keyword in wrecord:
            recisvalid = True
            break
    if not recisvalid:
        raise SurficialParserError(sc["replymessages"]["failweaen"])
    # except AttributeError:
    #     raise ValueError(c.reply.failweaen)
      
    # get all the name of reporter/s  
    try:
        observer_name = re.search("(?<="+wrecord+" ).+$",data_field).group(0)
        # print observer_name
    except AttributeError:
        raise SurficialParserError(sc["replymessages"]["failobven"])

    obv = dict()
    obv['ts']=  date_str+" "+time_str
    obv['meas_type'] = sms_list[0]
    obv['weather'] = wrecord
    obv['reliability'] = 1
    obv['data_source'] = 'SMS'
    obv['observer_name'] = observer_name

    obv['site_id'] = site_id
    
    data_records = dict()
    ind= 0
    for m in meas: #loop for each
        marker_name = m[0]
        marker_len = float(re.search("\d{1,3}\.*\d{0,2}",m[1:]).group(0))

        # check unit
        try:
            # centimeter
            re.search("\d *CM",m[1:]).group(0)
            multiplier = 1.00
        except AttributeError:
            # meter
            multiplier = 100.00

        marker_len = marker_len * multiplier
        
        # data_records.set_value(ind, 'marker_name', marker_name)
        # data_records.set_value(ind, 'measurement', cm)
        # ind = ind+1
        data_records[marker_name] = marker_len

    obv['data_records'] = data_records

    return obv

def get_time_from_sms(text):
  # timetxt = ""
    hm = "\d{1,2}"
    sep = " *:+ *"
    day = " *[AP]\.*M\.*"
  
    time_format_dict = {
        hm + sep + hm + day : "%I:%M%p",
        hm + day : "%I%p",
        hm + sep + hm + " *N\.*N\.*" : "%H:%M",
        hm + sep + hm + " +" : "%H:%M"
    }

    time_str = ''
    count = 0
    for fmt in time_format_dict:
        time_str_search = re.search(fmt,text)
        if time_str_search:
            time_str = time_str_search.group(0)
            time_str = re.sub(";",":",time_str)
            time_str = re.sub("[^APM0-9:]","",time_str)

            try:
                time_str = dt.strptime(time_str,time_format_dict[fmt]).strftime("%H:%M:%S")
            except ValueError:
                print 'match for', fmt, 'but error in conversion'
                raise SurficialParserError(sc["replymessages"]["failtimeen"])
            break
        # else:
        #     print 'not', fmt
        count += 1

    if count == len(time_format_dict):
        raise SurficialParserError(sc["replymessages"]["failtimeen"])
      
      # sanity check
    time_val = dt.strptime(time_str,"%H:%M:%S").time()
    # if time_val > dt.now().time():
    #     print 'Time out of bounds. Time too soon' 
    #     raise ValueError
    # el
    if (time_val > dt.strptime("18:00:00","%H:%M:%S").time() or time_val < 
        dt.strptime("05:00:00","%H:%M:%S").time()):
        print 'Time out of bounds. Unrealizable time to measure' 
        raise SurficialParserError(sc["replymessages"]["failtimeen"])

    return time_str

def adjust_site_code(site_code):
    translation_dict = {
        'man':'mng','bat':'bto','tag':'tga','pan':'png','pob':'jor'
    }

    if site_code in translation_dict.keys():
        return translation_dict[site_code]
    else:
        return site_code

        
def get_date_from_sms(text):
  # timetxt = ""
    mon_re1 = "[JFMASOND][AEPUCO][NBRYLGTVCP]"
    mon_re2 = "[A-Z]{4,9}"
    day_re1 = "(([0-3]{0,1})([0-9]{1}))"
    year_re1 = "(201[678]){0,1}"

    cur_year = str(dt.today().year)

    separator = "[\. ,]{0,3}"
    date_format_dict = {
        mon_re1 + separator + day_re1 + separator + year_re1 : "%b%d%Y",
        day_re1 + separator + mon_re1 + separator + year_re1 : "%d%b%Y",
        mon_re2 + separator + day_re1 + separator + year_re1 : "%B%d%Y",
        day_re1 + separator + mon_re2 + separator + year_re1 : "%d%B%Y"
    }

    date_str = ''
    count = 0
    for fmt in date_format_dict:
        date_str_search = re.search("^" + fmt,text)
        if date_str_search:
            date_str = date_str_search.group(0)
            date_str = re.sub("[^A-Z0-9]","",date_str).strip()
            # if len(date_str) < 4:
            #     date_str = date_str + cur_year 
            # else:
            #     print 'len of date(str) not < 4'
            print date_str
            try:
                date_str = dt.strptime(date_str,date_format_dict[fmt]).strftime("%Y-%m-%d")
            except ValueError:
                try:
                    date_str = date_str + cur_year 
                    date_str = dt.strptime(date_str,date_format_dict[fmt]).strftime("%Y-%m-%d")
                except ValueError:
                    raise SurficialParserError(sc["replymessages"]["faildateen"])
            break
        # else:
        #     print 'No match for', fmt
        count += 1

    # no match detected
    if count == len(date_format_dict):
        raise SurficialParserError(sc["replymessages"]["faildateen"])

    date_val = dt.strptime(date_str,"%Y-%m-%d")
    if date_val > dt.now():
        raise SurficialParserError(sc["replymessages"]["faildateen"])
    return date_str

def get_site_id(site_code):
    db, cur = dynadb.connect('local') 

    site_code = site_code.lower()
    
    if (site_code == 'man'):
        site_code= 'mng'
    elif (site_code == 'pan'):
        site_code= 'png'
    elif (site_code == 'pob'):
        site_code= "jor"
    elif (site_code == 'bat'):
        site_code= 'bto'
    elif (site_code == 'tag'):
        site_code= 'tga'
    # if (site_code == 'MES'): #(?)
    
    cur.execute('SELECT site_id FROM sites WHERE site_code = "{}"'.format(site_code))
    
    try:
        site_id = cur.fetchone()[0]
        db.close()
        return site_id
    except:
        print "ERROR in sites database"
        db.close()
        raise SurficialParserError('Error! There is a problem with your SITE CODE (%s). Please check your SMS and try again.' % site_code)
        # return None
        

def get_marker_id(site_id,marker_name):

    db, cur = dynadb.connect('local') 
    
    try:
        query = ("SELECT markers.marker_id FROM markers "
            "INNER JOIN marker_history ON markers.site_id = {} "
            "AND marker_history.marker_id = markers.marker_id "
            "INNER JOIN marker_names ON marker_history.history_id = " 
            "marker_names.history_id AND marker_names.marker_name "
            "= '{}'".format(site_id,marker_name))

        # print query

        cur.execute(query.format(site_id,marker_name))
        marker_id = cur.fetchone()[0]
        db.close()
        return marker_id
    except TypeError:
        marker_id = 0
        return marker_id

def insert_new_markers(site_id, marker_name, ts):
    db, cur = dynadb.connect() 
    # query = 'INSERT INTO markers (site_id) VALUES ({})'.format(site_id)
    # print query
    # cur.execute(query)
    cur.execute('INSERT INTO markers (site_id) VALUES ({})'.format(site_id))
    # marker_id = dbio.commit_to_db(query,'imd',last_insert=True)
    cur.execute('select last_insert_id()')
    marker_id = cur.fetchone()[0]
    print marker_id
    db.commit()

    history_query = ("INSERT INTO marker_history(marker_id,ts,event) "
        "VALUES (@@IDENTITY,'{}','add')".format(ts)
        )
    names_query = ('INSERT INTO marker_names(history_id,marker_name) '
        'VALUES(@@IDENTITY,"{}")'.format(marker_name)
        )
    cur.execute(history_query)
    db.commit()
    cur.execute(names_query)
    db.commit()        
    db.close()

    return marker_id

def update_surficial_observations(obv):
    
    db, cur = dynadb.connect('local') 

    # input marker_observation
    query = ("INSERT IGNORE INTO marker_observations "
        "(ts,meas_type,observer_name,reliability,weather,data_source, site_id) "
        "VALUES ('{}','{}','{}',{},'{}','{}',{})".format(obv['ts'],
            obv['meas_type'], obv['observer_name'], obv['reliability'],
            obv['weather'], obv['data_source'],obv['site_id'])
        )

    mo_id = dynadb.write(query, 'uso', last_insert=True)[0][0]

    # check if entry is duplicate
    if mo_id == 0:
        # print 'Duplicate entry'

        query = ("SELECT marker_observations.mo_id FROM marker_observations "
            "WHERE ts = '{}' and site_id = '{}'".format(obv['ts'],
            obv['site_id'])
            )    
        mo_id = dynadb.read(query,'uso')[0][0]

    return mo_id

def update_surficial_data(obv, mo_id):
    db, cur = dynadb.connect('local') 

    data_records = obv['data_records']

    for marker_name in data_records.keys():
        marker_id = get_marker_id(obv['site_id'], marker_name)
        if marker_id == 0:
            print "Inserting new marker ID"
            marker_id = insert_new_markers(obv['site_id'], marker_name, 
                obv['ts'])   

        query= ("INSERT INTO marker_data(marker_id,measurement,mo_id) "
            "VALUES({},{},{}) ON DUPLICATE KEY UPDATE "
            "MEASUREMENT = {}".format(marker_id, data_records[marker_name], 
            mo_id, data_records[marker_name])
            )

        dynadb.write(query,'usd')

    db.close()