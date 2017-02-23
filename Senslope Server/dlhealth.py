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
    elif (type== "start"):
        query= '''SELECT logger_health.logger_id, name, model_id from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case !=5 and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
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
    lgrdf= getSitesInfo("active","start")
    timeNow =  dt.today()

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
            print "res: " + str(res)
            encodeCase(timeNow, logger_id, res)

def checkLastActive(yy, mo, dd, hh, mm,  lgr_name):
    db, cur = dbio.SenslopeDBConnect('local')
    query = """select timestamp from """ + lgr_name + """ order by timestamp desc limit 1 """
    timeNow= dt.today()
    try:    
        a = cur.execute(query)
        if a:
            out = cur.fetchall()         
            for i in range(0,len(out)):
                st=out[i]
                lastTime= st[0]
                tdelta  =timeNow- lastTime  
                print "timeNow"
                print timeNow 
                print "lastTime"
                print lastTime    
                print "tdelta"
                print tdelta
                if lastTime > timeNow:   
                    return 1
                elif tdelta.seconds/60 < 59:
                    return 1
                elif tdelta.days < 2 and tdelta.seconds/60  > 60:
                    return 20
                else:
                    return 2  

    except MySQLdb.ProgrammingError:     
        return 2  
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
    print "logger_id"
    print logger_id

    print "prevState"
    print prevState
    print "case"
    print case

    if ( int(prevState) == int(case)):
        print "same status"
        query = """UPDATE logger_health SET ts_updated ='""" +str(timestamp)+ """' WHERE health_id=""" + prevCaseId
        dbio.commitToDb(query, 'logger_health')
        
    else:
        query = "INSERT into logger_health (health_case, logger_id, ts, ts_updated) values ('%d','%d','%s', '%s')" %(case, logger_id, timestamp, timestamp) 
        print "new entry"
        dbio.commitToDb(query, 'logger_health')

def checkCaseBasedFromInbox(): #okay
    lgrdf= getSitesInfo("inbox","id/contact")
    db, cur = dbio.SenslopeDBConnect('gsm')
    timeNow =  dt.today()
    
    for i in range (0, len(lgrdf)):
        s_number= str(lgrdf.sim_num.loc[i])        
        query = "SELECT sms_msg from smsinbox where sms_id > (select max(sms_id)-10000 from smsinbox) and sim_num = " + s_number + """ and (sms_msg like "%NO DATA PARSED%" or sms_msg like "%NO DATA FROM SENSELOPE%" or sms_msg like "%MANUAL RESET%") order by sms_id desc"""
        try:
            mesdf = psql.read_sql(query,db)    
        except pd.io.sql.DatabaseError,e:
            mesdf= 0

        tempStr= str(mesdf)
        tempStr= tempStr.lower()
        
        if (tempStr.find("no data parsed"))> 0:
            health= 7
        if (tempStr.find("nodataparsed"))> 0:
            health= 7
        elif (tempStr.find("no data from senselope"))> 0:
            health= 6
        elif (tempStr.find("nodatafromsenselope"))> 0:
            health= 6
        elif (tempStr.find("nodatafromsenslope"))> 0:
            health= 6
        elif (tempStr.find("manual reset"))> 0:
            health= 9
        elif (tempStr.find("manualreset"))> 0:
            health= 9
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
    timeNow =  dt.today()
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
        elif (logger_model > 9 and logger_model < 35 ): #ver3
            if (signal > 90):
                health = 14
            elif (batv1 < 12):
                health = 25
        else:
            health= 4
        print health
        encodeCase(timeNow, siteHealthdf.logger_id.loc[i], health)

    return 0
    
def gsmStatus():
    timeNow= dt.today()
    print timeNow

    db, cur = dbio.SenslopeDBConnect('local')        
    lgrdf= getSitesInfo("netstat","id/contact") 
    gsmio.gsmInit('call')
    for i in range (0,len(lgrdf)):
        s_number= str(lgrdf.sim_num.loc[i])
        t= time.strftime('%Y-%m-%d %H:%M:%S')
        s_number= s_number.replace("63","0",1)
        print s_number
        q= gsmio.callSite(s_number).replace("\r\n","")        
        if (q== "NO CARRIER"):
            health= 21
        elif (q== "BUSY" ):
            health= 22
        elif(q== "NO ANSWER"):
            health= 23
        elif(q== "NO DIALTONE"):
            health= 24
        encodeCase(timeNow, lgrdf.logger_id.loc[i], health)

def updatedeadSites(): #check itoooo
    db, cur = dbio.SenslopeDBConnect('local') 

    query= "SELECT logger_id FROM senslopedb.loggers where date_deactivated IS NOT NULL"
    lgrdf = psql.read_sql(query,db)
    # lagay sa dataframe
    timeNow =  dt.today()
        
    for i in range (0, len(lgrdf)):    
        logger_id= str(lgrdf.logger_id.loc[i])
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
    elif (health == 20):
        return "intermittent sites"
    elif (health == 21):
        return "No Carrier"
    elif (health == 22):
        return "Busy"
    elif (health == 23):
        return "No answer"
    elif (health == 24):
        return "No dial tone"
    elif (health == 25):
        return "v3 battery: not consistent"
def printloggerStatus():
    updatedeadSites()
    activeSites()
    checkCaseBasedFromInbox()
    healthCaseGenerator(storehealthData())

    mondir= c.fileio.monitoringoutputdir
    f = open(mondir+ "datalogger_health.txt", "w");
    f.seek(0)
    f.truncate()

    timeNow =  dt.today()
    f.write("Status update for: ")
    f.write(str(timeNow))
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
    
    for i in range(6,25):
        try:
            query= '''SELECT name from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where logger_health.health_case = '''+ str(i) + ''' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
            statdf = psql.read_sql(query,db)
            s= statdf[statdf.columns[0]]
            loggers= list(s)
            l= ', '.join(loggers)
            length= len(s)
            if (length > 1):
                title= "Maintenance Status (" + str(i) +"): "+ str(length)+ "\n" #kailangan ng description ng case
                f.write(title)
                f.write(str(caseDescription(i))) 
                f.write("\n")
                f.write(l)
                f.write("\n\n")

        except pd.io.sql.DatabaseError,e:
            statdf = 0

    
# def activeRain()







    f = open(mondir+"datalogger_health.txt", "r");
    for line in f.readlines():
        print line


def main():
    func = sys.argv[1] 
    if func == 'loggerstatus':
        printloggerStatus()
    elif func == 'checknetstat':
        print 'check net stat'
        gsmStatus()
    else:
        print '>> Wrong argument'

if __name__ == "__main__":
    main()