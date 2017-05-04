import re, sys
from datetime import datetime as dt
import cfgfileio as cfg
import subprocess, time
import senslopedbio as dbio
import pandas as pd

def parseGndData(text):
    c = cfg.config()
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
        
    data_field = re.split(" ",cleanText,maxsplit=2)[2]
    
    try:
        date_str = getDateFromSms(data_field)
        print "Date: " + date_str
    except ValueError:
        raise ValueError(c.reply.faildateen)
      
    try:
        time_str = getTimeFromSms(data_field)
        print "Time: " + time_str
    except ValueError:
        raise ValueError(c.reply.failtimeen)
      
    # get all the measurement pairs
    meas_pattern = "(?<= )[A-Z] *\d{1,3}\.*\d{0,2} *C*M"
    meas = re.findall(meas_pattern,data_field)
      # create records list
    if meas:
        pass
    else:
        raise ValueError(c.reply.failmeasen)
      
      # get all the weather information
    print repr(data_field)
    try:
        wrecord = re.search("(?<="+meas[-1]+" )[A-Z]+",data_field).group(0)
        recisvalid = False
        for keyword in ["ARAW","ULAN","BAGYO","LIMLIM","AMBON","ULAP","SUN","RAIN","CLOUD","DILIM","HAMOG"]:
            if keyword in wrecord:
                recisvalid = True
                print "valid"
                break
        if not recisvalid:
            raise AttributeError  
    except AttributeError:
        raise ValueError(c.reply.failweaen)
      
    # get all the name of reporter/s  
    try:
        observer_name = re.search("(?<="+wrecord+" ).+$",data_field).group(0)
        # print observer_name
    except AttributeError:
        raise ValueError(c.reply.failobven)
      
    ts=  date_str+" "+time_str
    meas_type = sms_list[0]
    site_code = sms_list [1]

    weather= wrecord
    reliability = 1
    data_source= 'SMS'
    marker_obervations= "("+ ts + ","+  meas_type +","+ observer_name +","+ str(reliability)+ ","+weather+","+data_source+ "," +site_code+ ")"
    
    data_records = pd.DataFrame(columns= ['marker_name', 'measurement', 'so_id'])
    ind= 0
    for m in meas: #loop for each
        try:
            marker_name = m.split(" ",1)[0]
            cm = m.split(" ",1)[1]
            print cm
        except IndexError:
            crid = m[0]
            cm = m[1:]

        try:
            re.search("\d *CM",cm).group(0)
            cm = float(re.search("\d{1,3}\.*\d{0,2}",cm).group(0))
        except AttributeError:
            # raise error unit not found. dont 
            cm = float(re.search("\d{1,3}\.*\d{0,2}",cm).group(0))*100.0
            
        data_records.set_value(ind, 'marker_name', marker_name)
        data_records.set_value(ind, 'measurement', cm)
        ind = ind+1

    return ts, meas_type, site_code, observer_name, weather, data_source, reliability, data_records

def getTimeFromSms(text):
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

    print text
    time_str = ''
    for fmt in time_format_dict:
        time_str_search = re.search(fmt,text)
        if time_str_search:
            time_str = time_str_search.group(0)
            time_str = re.sub(";",":",time_str)
            time_str = re.sub("[^APM0-9:]","",time_str)
            time_str = dt.strptime(time_str,time_format_dict[fmt]).strftime("%H:%M:%S")
            break
        else:
            print 'not', fmt
      
      # sanity check
    time_val = dt.strptime(time_str,"%H:%M:%S").time()
    if time_val > dt.now().time():
        raise ValueError
    elif time_val > dt.strptime("18:00:00","%H:%M:%S").time() or time_val < dt.strptime("05:00:00","%H:%M:%S").time(): 
        raise ValueError

    return time_str

        
def getDateFromSms(text):
  # timetxt = ""
    mon_re1 = "[JFMASOND][AEPUCO][NBRYLGTVCP]"
    mon_re2 = "[A-Z]{4,9}"
    day_re1 = "\d{1,2}"
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
    for fmt in date_format_dict:
        date_str_search = re.search("^" + fmt,text)
        if date_str_search:
            date_str = date_str_search.group(0)
            date_str = re.sub("[^A-Z0-9]","",date_str)
        if len(date_str) < 6:
            date_str = date_str + cur_year 
        date_str = dt.strptime(date_str,date_format_dict[fmt]).strftime("%Y-%m-%d")
        break

    date_val = dt.strptime(date_str,"%Y-%m-%d")
    if date_val > dt.now():
        raise ValueError          
    return date_str

def GetSiteID(code):
    db, cur = dbio.SenslopeDBConnect('local') 
    
    if (code == 'MAN'):
        code= 'MNG'
    if (code == 'PAN'):
        code= 'PNG'
    if (code == 'POB'):
        code= "JOR"
    if (code == 'BAT'):
        code= 'BTO'
    # if (code == 'MES'): #(?)
    
    cur.execute('SELECT site_id FROM sites WHERE site_code = "{}"'.format(code))
    
    try:
        site_id = cur.fetchone()[0]
        db.close()
        return site_id
    except:
        print "ERROR in sites database"
        # raise error
        db.close()



def GetMarkerID(site_id,marker_name,lat_long = None): 
    db, cur = dbio.SenslopeDBConnect('local') 
    
    # if lat_long == None and marker_name == None:
        # print "ERROR specify lat long or marker name"
    
    # else:
    try:
        cur.execute('SELECT markers.marker_id FROM markers INNER JOIN marker_history ON markers.site_id = {} AND marker_history.marker_id = markers.marker_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id AND marker_names.marker_name = "{}"'.format(site_id,marker_name))
        marker_id = cur.fetchone()[0]
        print "first try"
        db.close()
        return marker_id
    except:
        print "except"
        marker_id = 0
        return marker_id

def InsertNewMarkers(site_id, marker_name, description, latitude, longitude, ts):
    db, cur = dbio.SenslopeDBConnect('local') 
    cur.execute('INSERT INTO markers (description, latitude, longitude, in_use, site_id) VALUES ("{}",{},{},1,{})'.format(description,latitude,longitude,site_id))
    db.commit()
    cur.execute('INSERT INTO marker_history(marker_id,ts,event) VALUES (@@IDENTITY,"{}","add")'.format(ts))
    db.commit()
    cur.execute('INSERT INTO marker_names(history_id,marker_name) VALUES(@@IDENTITY,"{}")'.format(marker_name))
    db.commit()        
    db.close()
  
def UpdateSurficialObservations(ts, meas_type, site_code, observer_name, weather, data_source, reliability, data_records):
    ts, meas_type, site_code, weather, data_records
    
    db, cur = dbio.SenslopeDBConnect('local') 

    site_id= GetSiteID(site_code) #site code translator for man, mng, mes, pan, png, 
    so_id=0

    # input marker_observation
    try:
        cur.execute('SELECT marker_observations.mo_id FROM marker_observations WHERE ts = "{}" AND meas_type = "{}" AND observer_name = "{}" AND reliability = {} AND weather = "{}" AND data_source = "{}"'.format(ts,meas_type,observer_name,reliability,weather,data_source))    
        so_id =cur.fetchone()[0]
        print "Duplicate entry check mo_id = {}".format(so_id)
    except:
        cur.execute('INSERT INTO marker_observations(ts,meas_type,observer_name,reliability,weather,data_source, site_id) VALUES ("{}","{}","{}",{},"{}","{}",{})'.format(ts,meas_type,observer_name,reliability,weather,data_source, site_id))
        db.commit()

    # check for the marker observation id   
    try:
        cur.execute('SELECT marker_observations.mo_id FROM marker_observations WHERE ts = "{}" AND meas_type = "{}" AND observer_name = "{}" AND reliability = {} AND weather = "{}" AND data_source = "{}"'.format(ts,meas_type,observer_name,reliability,weather,data_source))    
        so_id =cur.fetchone()[0]
    except:
        print 'i should raise value error!'
        # raise
    print data_records
 
    if (so_id==0):
        print "raise error na walang record pa"
    else:
        print data_records
        UpdateSurficialData(ts, site_id, so_id, data_records)

   
def UpdateSurficialData(ts, site_id, so_id, data_records):
    db, cur = dbio.SenslopeDBConnect('local') 

    print so_id
    print data_records
    print len(data_records)
    print "before error"

    for i in range(0,len(data_records)):
        # globaldf.name.loc[i]
        marker_id= GetMarkerID(site_id, data_records.marker_name.loc[i], lat_long = None)
        
        if (marker_id == 0):
            InsertNewMarkers(site_id, data_records.marker_name.loc[i], "No description", "null", "null", ts)
            print "inserting new marker details"

        marker_id= GetMarkerID(site_id, data_records.marker_name.loc[i], lat_long = None)
        print "so_id"
        print so_id
          
        data_records.set_value(i, 'marker_name', marker_id)
        data_records.set_value(i, 'so_id', so_id)

    print data_records

    if (len(data_records) != 0):
        for i in range (0,len(data_records)):
            mi=data_records.marker_name.loc[i]
            meas=data_records.measurement.loc[i]
            mo=data_records.so_id.loc[i]
            
            try:
                print "lagay na ng data"
                print mi, meas, mo

                query= 'INSERT INTO marker_data(marker_id,measurement,mo_id) VALUES({},{},{}) ON DUPLICATE KEY UPDATE MEASUREMENT= {}'.format(mi, meas, mo, meas)
                cur.execute(query)
            except:
                print "Error"

        db.commit()
        db.close()


# think of an algo that will run thisssss




        # try:
        #     cur.execute('SELECT markerdata_id FROM marker_data WHERE mo_id = {} AND marker_id = {} '.format(data_records.so_id.loc[i], data_records.marker_name.loc[i]))    
        #     a=cur.fetchall()
        #     print a
        #     #data_records.drop(data_records.index[[i]], inplace= True)
        #     #print "data_Records"

        #     #print data_records
        
        # except:
        #     print "nai"
        