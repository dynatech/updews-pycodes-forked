import os,time,serial,re,sys,traceback
import MySQLdb, subprocess
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio
import groundMeasurements as gndmeas
import SomsServerParser as SSP
import senslopeServer as server
import cfgfileio as cfg
import argparse
import queryserverinfo as qsi
import lockscript as lock
import alertmessaging as amsg 
import pandas as psql
import gsmSerialio as gsmio
from time import localtime, strftime
import pandas as pd
import cfgfileio as cfg

df= 0
casedf= 0
siteHealthdf=0
statdf= 0

c = cfg.config()
    
def getSitesInfo(status, type):
    db, cur = dbio.SenslopeDBConnect('local') 
    case= 0
    if (status == "active"):
        case = 1
    elif (status == "inbox"):
        case = 2
    elif (status== "health"):
        case= 3
    elif (status == "netstat"):
        case= 4
    if (type== "id/name"):
        query= '''SELECT logger_health.logger_id, name from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case = '''+ str(case) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    elif (type== "name"):
        query= '''SELECT name from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case = '''+ str(case) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    elif (type== "id"):
        query= '''SELECT logger_id from logger_health where logger_health.health_case = '''+ str(case) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    elif (type== "id/contact"):
        query= '''SELECT logger_health.logger_id, sim_num from logger_health inner join logger_contacts on logger_health.logger_id= logger_contacts.logger_id where logger_health.health_case = ''' + str(case) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    elif (type== "id/name/model"):
        query= '''SELECT logger_health.logger_id, name, model_id from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case = '''+ str(case) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    elif (type== "id/model"):
        query= '''SELECT logger_health.logger_id, model_id from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case = '''+ str(case) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    else:
        query=""
    try:
        statdf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        statdf = 0
    return statdf

def activeSites(): 
    lgrdf= getSitesInfo("active","id/name/model")
    timeNow =  strftime("%Y-%m-%d %H:%M:%S", localtime())

    yy= strftime("%Y", localtime())
    mo= strftime("%m", localtime())
    dd= strftime("%d", localtime())
    hh= strftime("%H", localtime())
    mm= strftime("%M", localtime())

    for i in range (0, len(lgrdf)):
        lgr_name= str (lgrdf.name.loc[i])
        logger_id= int(lgrdf.logger_id.loc[i])
        logger_model= int(lgrdf.model_id[i])

        if (logger_model < 27):
            res= checkLastActive(yy, mo, dd, hh, mm, lgr_name)
            if (res==1):
                encodeCase(timeNow, logger_id, 1)
            else:
                encodeCase(timeNow, logger_id, 2)

def checkLastActive(yy, mo, dd, hh, mm,  lgr_name):
    db, cur = dbio.SenslopeDBConnect('local')
    query = """select timestamp from """ + lgr_name + """ order by timestamp desc limit 1 """
    try:    
        a = cur.execute(query)
        if a:
            out = cur.fetchall()         
            for i in range(0,len(out)):
                st=out[i]
                lastTime= st[0]

                print lastTime
                
                lyy= lastTime.strftime('%Y')
                lmo= lastTime.strftime('%m')
                ldd= lastTime.strftime('%d')
                lhh= lastTime.strftime('%H')

                if ((int(yy)- int(lyy)) == 0):
                    if ((int(mo)- int(lmo)) == 0):
                        if ((int(dd)- int(ldd)) == 0):
                            if ((int(hh)- int(lhh)) <= 1):
                                print 'active'
                                return 1     
                            else:
                                print '1 hr'
                                #kapag more than 1 hr na, check niya kung ano yung dahilan
                                return 0
                        else:
                            print '1 day na :o <- pero check kasi baka 8 hrs lang ganyan'
                            return 0    
                    else:
                        print 'more than 1 month na'
                        return 0                                
                else:
                    print 'more than 1 year na'
                    return 0
        else:
            print '>> not listed in database'
    # except MySQLdb.OperationalError, MySQLdb.ProgrammingError:
    except MySQLdb.ProgrammingError:     
        print 'i caught the error! yeah'
        a =  None    
    db.close()

def encodeCase(timestamp, logger_id, case): #okay
    db, cur = dbio.SenslopeDBConnect('local') 
    query = '''SELECT health_id, health_case from logger_health where logger_id = ''' + str(logger_id) + ''' order by health_id desc limit 1'''
    casedf = psql.read_sql(query,db)
    
    try:
        prevState = str(casedf.health_case.loc[0])
        prevCaseId = str(casedf.health_id.loc[0])
    
    except KeyError,e:
        prevState= 0
    print "checking prev and current case"
    
    if ( int(prevState) == int(case)):
        print "same status"
        query = """UPDATE logger_health SET ts_updated ='""" +timestamp+ """' WHERE health_id=""" + prevCaseId
        dbio.commitToDb(query, 'logger_health')
        
    else:
        query = "INSERT into logger_health (health_case, logger_id, ts, ts_updated) values ('%d','%d','%s', '%s')" %(case, logger_id, timestamp, timestamp) 
        print "new entry"
        dbio.commitToDb(query, 'logger_health')

def checkCaseBasedFromInbox(): #okay
    lgrdf= getSitesInfo("inbox","id/contact")
    db, cur = dbio.SenslopeDBConnect('gsm')
    timeNow =  strftime("%Y-%m-%d %H:%M:%S", localtime()) #not yet finished; may use a function to get current time
    
    for i in range (0, len(lgrdf)):
        s_number= str(lgrdf.sim_num.loc[i])        
        query = "SELECT sms_msg from smsinbox where sms_id > (select max(sms_id)-10000 from smsinbox) and sim_num = " + s_number + """ and (sms_msg like "%NO DATA PARSED%" or sms_msg like "%NO DATA FROM SENSELOPE%" or sms_msg like "%MANUAL RESET%") order by sms_id desc"""
        try:
            mesdf = psql.read_sql(query,db)    
        except pd.io.sql.DatabaseError,e:
            mesdf= 0

        tempStr= str(mesdf)
        
        if (tempStr.find("no data parsed"))> 0:
            health= 7
        elif (tempStr.find("NO DATA FROM SENSELOPE"))> 0:
            health= 6
        elif (tempStr.find("MANUAL RESET"))> 0:
            health= 8
        else:
            health= 3
        print health
        encodeCase(timeNow, lgrdf.logger_id.loc[i], health)

def storehealthData():
    db, cur = dbio.SenslopeDBConnect('local')    
    lgrdf= getSitesInfo("health","id/name/model") #CHECK DATA DAPAT
    
    print lgrdf
    columns= ['logger_id','batv1', 'batv2', 'signal', 'model']

    siteHealthdf = pd.DataFrame(columns=columns)
    
    print siteHealthdf

    for i in range (0, len(lgrdf)):
        lgr_name= str(lgrdf.name.loc[i])
        logger_id= int(lgrdf.logger_id.loc[i])
        logger_model= int(lgrdf.model_id[i])

        if (logger_model > 1 and logger_model < 10):
            query = '''SELECT avg(batv1),avg(batv2),avg(csq) from ''' + lgr_name + 'w'+ ''' order by timestamp desc limit 48'''
            print query
            #average lang ito, dapat sana trendin, pero next time na powsz
            try:
                tempdf = psql.read_sql(query,db)
                print tempdf
                print tempdf.loc[0, 'avg(batv1)']
                print tempdf.loc[0, 'avg(batv2)']
                print tempdf.loc[0, 'avg(csq)']

                siteHealthdf.set_value(i, 'batv1', tempdf.loc[0, 'avg(batv1)'])
                siteHealthdf.set_value(i, 'batv2', tempdf.loc[0, 'avg(batv2)'])
                siteHealthdf.set_value(i, 'signal', tempdf.loc[0, 'avg(csq)'])
                

            except pd.io.sql.DatabaseError,e:
               tempdf= 0
        
        elif (logger_model > 9 and logger_model < 35 ):
            sitecode= lgr_name[:3]
            # sitecode[0]= lgr_name[0]
            # sitecode[1]= lgr_name[1]
            # sitecode[2]= lgr_name[2]
            # sitecode[3]= '\0'             

            query = '''SELECT avg(batv1),avg(csq) from ''' + lgr_name + 'w'+ ''' order by timestamp desc limit 48'''
            print query

            try:
                tempdf = psql.read_sql(query,db)
                print tempdf
                print tempdf.loc[0, 'avg(batv1)']
                print tempdf.loc[0, 'avg(csq)']

                siteHealthdf.set_value(i, 'batv1', tempdf.loc[0, 'avg(batv1)'])
                siteHealthdf.set_value(i, 'signal', tempdf.loc[0, 'avg(csq)'])
            except pd.io.sql.DatabaseError,e:
                tempdf = 0
        
        print tempdf
        siteHealthdf.set_value(i, 'model', logger_model)
        siteHealthdf.set_value(i, 'logger_id', logger_id)

    print 'siteHealthdf from storehealthData'
    print siteHealthdf  
    return siteHealthdf

def healthCaseGenerator(siteHealthdf):
    timeNow =  strftime("%Y-%m-%d %H:%M:%S", localtime())
    print siteHealthdf
    health= 4
    for i in range (0, len(siteHealthdf)):
        ver = int (siteHealthdf.model.loc[i])
        batv1= float(siteHealthdf.batv1.loc[i])
        batv2= float(siteHealthdf.batv2.loc[i])
        signal= float(siteHealthdf.signal.loc[i])
        logger_id= str(siteHealthdf.logger_id.loc[i])
        logger_model= int(siteHealthdf.model.loc[i])
 
        if (logger_model > 1 and logger_model < 10): #arq
            if (signal < 10):
                health = 14
            elif (batv1 < 3.3 and batv2 < 3.3):
                health = 12
        elif (logger_model > 9 and logger_model < 35 ): #
            if (signal > 90):
                health = 14
            elif (batv1 < 12):
                health = 12
        else:
            health= 4
        print health
        encodeCase(timeNow, siteHealthdf.logger_id.loc[i], health)

    return 0


# def checkNetStat(df):
#     getVersion()
#     if ver== 2
#         if result == ""
#             check network na
#         else reset_site()
#     if ver ==3
#         if result == ""
#             check network
#         else 
#             reset_site()


    
# def gsmStatus(df, table= "site_gsm_status"):
#     db, cur = dbio.SenslopeDBConnect('local')        
#     dbname= getDatabase('local')
    
#     for i in range (0,len(df)):
#         s_number= "0" + str(df.sim_num.loc[i])
#         s_name= str(df.s_name.loc[i])
#         t= time.strftime('%Y-%m-%d %H:%M:%S')  
#         q= gsmio.callSite(s_number).replace("\r\n","")
       
#         query = "INSERT INTO " + table + ''' VALUES ( "''' + t + '''" , "''' + s_name + '''" , "''' + s_number + '''" , "''' + q + '''")'''
#         cur.execute(query)
#         db.commit()
#         print query
#     return df

def updatedeadSites(): #check itoooo
    db, cur = dbio.SenslopeDBConnect('local') 

    query= "SELECT logger_id FROM senslopedb.loggers where date_deactivated IS NOT NULL"
    lgrdf = psql.read_sql(query,db)
    # lagay sa dataframe
    for i in range (0, len(lgrdf)):    
        logger_id= str(lgrdf.logger_id.loc[i])
        timeNow =  strftime("%Y-%m-%d %H:%M:%S", localtime())
        health= 5
        encodeCase(timeNow, lgrdf.logger_id.loc[i], health)

def caseDescription(health):
    if (health == 6):
        return"No data from senslope"
    elif (health == 7):
        return "No data parsed"    
    elif (health == 8):
        return "trash"
    elif (health == 9):
        return "Manual reset"
    elif (health == 10):
        return "Date only"
    elif (health == 11):
        return "-"
    elif (health == 12):
        return "Low battery"
    elif (health == 13):
        return "RSSI is greater than 90"
    elif (health == 14):
        return "GSM signal is lower than 10"
    elif (health == 15):
        return "Network error"
    elif (health == 16):
        return "Raspberry crashed"
    elif (health == 17):
        return "No issue with the arq"
    elif (health == 18):
        return "Not responding with sensorpoll"
    elif (health == 19):
        return "Arq with issue, RTC"

def printloggerStatus():

    activeSites()
    checkCaseBasedFromInbox()
    healthCaseGenerator(storehealthData())
    updatedeadSites()

    mondir= c.fileio.monitoringoutputdir
    f = open(mondir+ "datalogger_health.txt", "w");
    f.seek(0)
    f.truncate()

    timeNow =  strftime("%Y-%m-%d %H:%M:%S", localtime())
    f.write("Status update for: ")
    f.write(timeNow)
    f.write("\n\n\n")
    print "Active Sites:"
    df= getSitesInfo("active","name")

    s= df[df.columns[0]]
    loggers= list(s)
    l= ', '.join(loggers)
    print l
    title= "Active sites: " + str(len(df))+ "\n"
    f.write(title)
    f.write(l)
    f.write("\n\n")

    print "Sites need to check the status of their networks"
    df= getSitesInfo("netstat","name")
    s= df[df.columns[0]]
    loggers= list(s)
    l= ', '.join(loggers)
    print l
    title= "Sites need to check the status of their networks: " + str(len(df))+ "\n"
    f.write(title)
    f.write(l)
    f.write("\n\n")

    db, cur = dbio.SenslopeDBConnect('local')  
    
    for i in range(6,19):
        try:
            query= '''SELECT name from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case = '''+ str(i) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
            statdf = psql.read_sql(query,db)
            # print "Maintenance Status: " + str(i)
            s= statdf[statdf.columns[0]]
            loggers= list(s)
            l= ', '.join(loggers)
            length= len(s)
            if (length > 1):
                title= "Maintenance Status (" + str(i) +"): "+ str(length)+ "\n" #kailangan ng description ng case
                f.write(title)
                f.write(caseDescription(i)) 
                f.write("\n")
                f.write(l)
                f.write("\n\n")

        except pd.io.sql.DatabaseError,e:
            statdf = 0

        
    f = open(mondir+"datalogger_health.txt", "r");
    for line in f.readlines():
        print line

printloggerStatus()
