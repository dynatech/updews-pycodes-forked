import re
from datetime import datetime as dt
import ConfigParser

def getGndMeas(text):

  cfg = ConfigParser.ConfigParser()
  cfg.read('senslope-server-config.txt')

  print '\n\n*******************************************************'  
  faildateen = cfg.get('ReplyMessages','FailDateEN')
  failtimeen = cfg.get('ReplyMessages','FailTimeEN')
  failmeasen = cfg.get('ReplyMessages','FailMeasEN')
  failweaen = cfg.get('ReplyMessages','FailWeaEN')
  failobven = cfg.get('ReplyMessages','FailObvEN')

  print text
  
  # clean the message
  cleanText = re.sub(" +"," ",text.upper())
  sms_list = re.split(" ",re.sub("\W"," ",cleanText))
  
  sms_date = ""
  sms_time = ""
  records = []
  
  # check measurement type
  if sms_list[0][0] == 'R':
    meas_type = "ROUTINE"
  else:
    meas_type = "EVENT"
  
  # check date
  date_search = re.search("[JFMASOND][A-Z]{2} *\d{1,2} *(201[67]){0,1}",cleanText)
  if date_search:
    date_str = date_search.group(0).replace(" ","")
    if len(date_str) < 6:
        date_str = date_str + "2016"        
    print date_str
    try:
        sms_date = dt.strptime(date_str,"%b%d%Y").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(faildateen)
  else:
    raise ValueError(faildateen)
  
  # check time
  # if 
  sms_time_search = re.search("\d{1,2}\:{0,1}\d{0,2}[AP]M", cleanText)
  if sms_time_search:
    sms_time_str = sms_time_search.group(0)
    if sms_time_str.find(':') > 0:
      sms_time = dt.strptime(sms_time_str,"%I:%M%p").strftime("%H:%M:00")
    else:
      sms_time = dt.strptime(sms_time_str,"%I%p").strftime("%H:%M:00")
  else:
    raise ValueError(failtimeen)
  
  # get all the measurement pairs
  meas_pattern = "[A-Z] \d{1,3}\.*\d{0,2}C*M"
  meas = re.findall(meas_pattern,cleanText)
  # create records list
  if meas:
    pass
  else:
    raise ValueError(failmeasen)
  
  try:
    wrecord = re.search("(?<="+meas[-1]+" )\w+(?= )",cleanText).group(0)
    print wrecord
  except AttributeError:
    raise ValueError(failweaen)
    
  try:
    observer_name = re.search("(?<="+wrecord+" ).+$",cleanText).group(0)
    print observer_name
  except AttributeError:
    raise ValueError(failobven)
    
  gnd_records = ""
  for m in meas:
    crid = m.split(" ")[0]
    cm = m.split(" ")[1]
    try:
      re.search("\dM",cm).group(0)
      cm = float(re.search("\d{1,3}\.*\d{0,2}",cm).group(0))*100.0
    except AttributeError:
      cm = float(re.search("\d{1,3}\.*\d{0,2}",cm).group(0))
      
    gnd_records = gnd_records + "('"+sms_date+" "+sms_time+"','"+sms_list[0]+"','"+sms_list[1]+"','"+observer_name+"','"+crid+"','"+str(cm)+"'),"
    
  gnd_records = gnd_records[:-1]
  
  wea_desc = "('"+sms_date+" "+sms_time+"','"+meas_type+"','"+sms_list[1]+"','"+observer_name+"','"+wrecord+"')"
  
  return gnd_records, wea_desc
  

  
  
