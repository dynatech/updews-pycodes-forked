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
import numpy as np

from time import localtime, strftime

import pandas as pd
import cfgfileio as cfg

df= 0
casedf= 0
siteHealthdf=0
statdf= 0


globaldf= pd.DataFrame()

c = cfg.config()

def readDataframe():
    
    # global globaldf
    localdf=0
    db, cur = dbio.SenslopeDBConnect('local')    
    query= '''SELECT logger_health.logger_id, name, model_id, sim_num, health_case from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id inner join logger_contacts on logger_health.logger_id= logger_contacts.logger_id where logger_health.health_case !=5 and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)'''
    
    try:
        localdf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        localdf = 0
    return localdf

def activeSites(): 
    global globaldf
    globaldf= readDataframe() 

    for i in range (0, len(globaldf)):
        lgr_name= str (globaldf.name.loc[i])
        logger_id= int(globaldf.logger_id.loc[i])
        logger_model= int(globaldf.model_id[i])
        case= int(globaldf.health_case[i])

        if (logger_model < 27):    
            res= checkLastActive(lgr_name)
            
            if (case > 5 and case <20):
                if (res == 1 or res==20):
                    globaldf.set_value(i, 'health_case', res)
                else:
                    globaldf.set_value(i, 'health_case', case)
            elif (case > 20 and case <25):
                globaldf.set_value(i, 'health_case', case)
            elif (case== 4):
                globaldf.set_value(i, 'health_case', case)
            else:
                globaldf.set_value(i, 'health_case', res)
                
def checkLastActive(lgr_name):
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

                if lastTime > timeNow:   
                    return 1
                elif tdelta.seconds/60 < 59:
                    return 1
                elif tdelta.days < 2 and tdelta.seconds/60  > 60:
                    return 20
                else:
                    return 2  

    except MySQLdb.ProgrammingError:     
        return 50  
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

    if ( int(prevState) == int(case)):
        query = """UPDATE logger_health SET ts_updated ='""" +str(timestamp)+ """' WHERE health_id=""" + prevCaseId
    elif (((prevState < 5) or (case < 5)) and ((prevState != 1) or (case != 1))):
        query = """UPDATE logger_health SET ts_updated = '""" +str(timestamp)+ """' , health_case= """ + str(case) + " WHERE health_id= " + prevCaseId
    else:
        query = "INSERT into logger_health (health_case, logger_id, ts, ts_updated) values ('%d','%d','%s', '%s')" %(case, logger_id, timestamp, timestamp) 
    dbio.commitToDb(query, 'logger_health')

def checkCaseBasedFromInbox(): #okay
    global globaldf
    
    db, cur = dbio.SenslopeDBConnect('gsm')
    
    for i in range (0, len(globaldf)):
        if (globaldf.health_case.loc[i] == 2):
            s_number= str(globaldf.sim_num.loc[i])        
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
                health= 4
            globaldf.set_value(i, 'health_case', health)        
            
def storehealthData():
    global globaldf 

    db, cur = dbio.SenslopeDBConnect('local')    
    columns= ['logger_id','batv1', 'batv2', 'signal', 'model']
    siteHealthdf = pd.DataFrame(columns=columns)

    for i in range (0, len(globaldf)):
        if (globaldf.health_case.loc[i] > 20 and globaldf.health_case.loc[i] < 25):
            lgr_name= str(globaldf.name.loc[i])
            logger_id= int(globaldf.logger_id.loc[i])
            logger_model= int(globaldf.model_id[i])

            if (logger_model > 1 and logger_model < 10):
                query = '''SELECT avg(batv1),avg(batv2),avg(csq) from ''' + lgr_name + 'w'+ ''' order by timestamp desc limit 48'''
                #average lang ito, dapat sana trendin, pero next time na powsz
                try:
                    tempdf = psql.read_sql(query,db)

                    siteHealthdf.set_value(i, 'batv1', tempdf.loc[0, 'avg(batv1)'])
                    siteHealthdf.set_value(i, 'batv2', tempdf.loc[0, 'avg(batv2)'])
                    siteHealthdf.set_value(i, 'signal', tempdf.loc[0, 'avg(csq)'])
                    
                except pd.io.sql.DatabaseError,e:
                   tempdf= 0
            
            elif (logger_model > 9 and logger_model < 35 ):
                sitecode= lgr_name[:3]    

                query = '''SELECT avg(batv1),avg(csq) from ''' + lgr_name + 'w'+ ''' order by timestamp desc limit 48'''
       

                try:
                    tempdf = psql.read_sql(query,db)

                    siteHealthdf.set_value(i, 'batv1', tempdf.loc[0, 'avg(batv1)'])
                    siteHealthdf.set_value(i, 'signal', tempdf.loc[0, 'avg(csq)'])
                except pd.io.sql.DatabaseError,e:
                    tempdf = 0
    
            siteHealthdf.set_value(i, 'model', logger_model)
            siteHealthdf.set_value(i, 'logger_id', logger_id)
    return siteHealthdf

def healthCaseGenerator(siteHealthdf):
    global globaldf
    print siteHealthdf

    health= 3
    
    for i in range (0, len(globaldf)):
        print i
        try:
            ver = int (siteHealthdf.model.loc[i])
            batv1= float(siteHealthdf.batv1.loc[i])
            batv2= float(siteHealthdf.batv2.loc[i])
            signal= float(siteHealthdf.signal.loc[i])
            logger_id= str(siteHealthdf.logger_id.loc[i])
            logger_model= int(siteHealthdf.model.loc[i])
            # print "PUMASOK!!!!!!! "
            # baka walang laman :()

            if (globaldf.health_case.loc[i] == 21): #no carrier/ cannot be reached    
                print "no carrier"
                if (logger_model > 1 and logger_model < 10): #arq
                    if (batv1 < 3.3 and batv2 < 3.3):
                        health = 12
                    elif (signal<10):
                        health = 14
                    else:
                        health= 3
                
                if (logger_model > 9 and logger_model < 18):
                    if (batv1< 13):
                        health= 12
                    elif (signal<10):
                        health = 14
                    else:
                        health= 3

            elif (globaldf.health_case.loc[i] == 22): #busy/ binaba
                if (logger_model > 1 and logger_model < 10):
                    health= 11
                elif (batv1): #doublecheck
                    health= 12
                elif (logger_model > 17 and logger_model < 27): #regular    (walang case ng mababang csq dito)
                    health=11
                else:
                    health= 3
                
            elif (globaldf.health_case.loc[i] == 23): #no answer/ ringing
                if (logger_model > 1 and logger_model < 10): #arq
                    health = 11
                elif (batv1 < 13):
                    health=12
                elif (logger_model > 17 and logger_model < 27): 
                    health =11                     
                else: #yung mahaba
                    health =3
            elif (globaldf.health_case.loc[i] == 24): #no dialtone
                health=3
            else:
                health=3
            globaldf.set_value(i, 'health_case', health)
        except KeyError, e:
            print ""  
    return 0
    
def gsmStatus(): #ibaaaaang approach
    timeNow= dt.today()
    lgrdf= readDataframe()
    # global lgrdf 
    gsmio.gsmInit('call')
    for i in range (0,len(lgrdf)):
        if (lgrdf.health_case.loc[i] == 4): #3 na yung chinecheck
            if (int(lgrdf.model_id.loc[i])>9 and int(lgrdf.model_id.loc[i])<18): #kapag router, wag na tawagan (?)
                print "not calling routers"
            else:
                s_number= str(lgrdf.sim_num.loc[i])
                s_id= int(lgrdf.logger_id.loc[i])
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
                else:
                    health= 4
                lgrdf.set_value(i, 'health_case', health)  
                encodeCase(timeNow, s_id, health)
def updatedeadSites():
    db, cur = dbio.SenslopeDBConnect('local') 
    query= "SELECT logger_id FROM senslopedb.loggers where date_deactivated IS NOT NULL"
    lgrdf = psql.read_sql(query,db)
    timeNow =  dt.today()
        
    for i in range (0, len(lgrdf)):    
        logger_id= str(lgrdf.logger_id.loc[i])
        health= 5
        encodeCase(timeNow, lgrdf.logger_id.loc[i], health)

def caseDescription(health):
    if (health == 2):
        return "Check inbox (no table/no recent text)"
    elif (health == 3):
        return "Check health"
    elif (health == 4):
        return "Check network status"
    elif (health == 5):
        return "Deads na"
    elif (health == 6):
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
        return "Ask LEWC to reset logger"
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
    # updatedeadSites()
    
    activeSites()
    checkCaseBasedFromInbox()
    healthCaseGenerator(storehealthData()) #<- ginagawang 4 yung iba o.O
    
    globaldf.sort_values(['name'], ascending=[True],inplace = True)
    globaldf.index = range(0,len(globaldf))
    print globaldf

    timeNow =  dt.today()
    
    mondir= c.fileio.monitoringoutputdir
    f = open(mondir+ "datalogger_health.txt", "w");
    f.seek(0)
    f.truncate()

    f.write("Status update for: ")
    f.write(str(timeNow))
    f.write("\n\n\n")

    count= globaldf["health_case"].value_counts()
    
    f.write("Active Loggers: ")
    f.write(str(count[1])) 
    f.write("\n")    

    for i in range (0,len(globaldf)):
        if (globaldf.health_case.loc[i] == 1):
            f.write(str(globaldf.name.loc[i]))
            f.write(" ")

    count= globaldf["health_case"].value_counts()
    for j in range(2,26):
        try:
            co= count[j]
            f.write('\n')
            f.write('\n')
            
            title= "For Maintenance Loggers (Status: " + str(j) +"): " + str(co) + "\n" 
            # print title
            f.write(title)
            descr=  caseDescription(j) + "\n" 
            f.write(descr)
            
            for i in range (0,len(globaldf)):             
                if (globaldf.health_case.loc[i] == j):               
                    f.write(str(globaldf.name.loc[i]))
                    f.write(" ")       
        except KeyError, e:
            print ""
    
    f.close
        
    f = open(mondir+"datalogger_health.txt", "r");
    for line in f.readlines():
        print line
    f.close

def encodeDataFrame(inputdf):
    print inputdf
    timeNow= dt.today()
    
    for i in range (0, len(inputdf)):
        # lgr_name= str (inputdf.name.loc[i])
        logger_id= int(globaldf.logger_id.loc[i])
        health= int(globaldf.health_case[i])
        encodeCase(timeNow, logger_id, health)

    # toLogHealthcols = ['logger_id','health_Case', 'ts', 'ts_updated']
    
    # INSERT INTO logger_health (logger_id,health_case,ts, timestamp) VALUES (1,1,1),(2,2,3),(3,9,3),(4,10,12)
    # ON DUPLICATE KEY UPDATE Col1=VALUES(Col1),Col2=VALUES(Col2);
    
    # siteHealthdf = pd.DataFrame(columns=columns)



def activeRain():
    #rain_tsm
    
    raindf=0
    
    db, cur = dbio.SenslopeDBConnect('local')
    query= '''SELECT name, loggers.model_id from loggers inner join (select model_id from logger_models where has_rain =1) a on a.model_id= loggers.model_id'''
    try:
        raindf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        raindf = 0

    for i in range (0, len(raindf)):
        lgr_name= str (raindf.name.loc[i])
        lgr_name= lgr_name + 'w'
        
        res= checkLastActive(lgr_name)

        raindf.set_value(i, 'model_id', res)    
    return raindf

def activeSoms():
    somsdf=0
    db, cur = dbio.SenslopeDBConnect('local')
    query= '''SELECT name, loggers.model_id from loggers inner join (select model_id from logger_models where has_soms =1) a on a.model_id= loggers.model_id'''
    try:
        somsdf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        somsdf = 0

    for i in range (0, len(somsdf)):
        lgr_name= str (somsdf.name.loc[i])
        lgr_name= lgr_name + 'm'
        
        res= checkLastActive(lgr_name)
        somsdf.set_value(i, 'model_id', res)    
    return somsdf

def activePiezo():
    piezodf=0
    db, cur = dbio.SenslopeDBConnect('local')
    query= '''SELECT name, loggers.model_id from loggers inner join (select model_id from logger_models where has_piezo =1) a on a.model_id= loggers.model_id'''
    
    try:
        piezodf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        piezodf = 0

    for i in range (0, len(piezodf)):
        lgr_name= str (piezodf.name.loc[i])
        lgr_name= lgr_name + 'pz'
        res= checkLastActive(lgr_name)
        if (res == 50):
            lgr_name= lgr_name + 'pz'  
            res= checkLastActive(lgr_name)
        else:
            print ""
            
        piezodf.set_value(i, 'model_id', res)    
   
    return piezodf


def printotherStatus():
    timeNow =  dt.today()
    
    raindf= activeRain()
    pzdf= activePiezo()
    somsdf = activeSoms()

    raindf.sort_values(['name'], ascending=[True],inplace = True)
    pzdf.sort_values(['name'], ascending=[True],inplace = True)
    somsdf.sort_values(['name'], ascending=[True],inplace = True)
    raindf.index = range(0,len(raindf))
    pzdf.index = range(0,len(pzdf))
    somsdf.index = range(0,len(somsdf))



    mondir= c.fileio.monitoringoutputdir
    f = open(mondir+ "otherSensors_health.txt", "w");
    f.seek(0)
    f.truncate()

    f.write("Status update for: ")
    f.write(str(timeNow))
    f.write("\n\n")
    writeStatus(f, "Rain", raindf)
    f.write("\n\n")
    writeStatus(f, "Piezo", pzdf) 
    f.write("\n\n")
    writeStatus(f, "Soil Moisture", somsdf) 
    

    f = open(mondir+"otherSensors_health.txt", "r");
    for line in f.readlines():
        print line

def writeStatus(f, sensortype, df):

    f.write("----------     ")
    f.write(str(sensortype))
    f.write("     ----------")
    f.write("\n\n")
    f.write("Active Loggers: ")
    count= df["model_id"].value_counts()
    f.write(str(count[1])) 
    f.write("\n")    
    for i in range (0,len(df)):
        if (df.model_id.loc[i] == 1):
            f.write(str(df.name.loc[i]))
            f.write(" ")

    f.write("\n\n")
    f.write("Intermittent: ")
    f.write(str(count[20])) 
    f.write("\n")    
    for i in range (0,len(df)):
        if (df.model_id.loc[i] == 20):
            f.write(str(df.name.loc[i]))
            f.write(" ")

    f.write("\n\n")
    f.write("For maintenance: ")
    f.write(str(count[2])) 
    f.write("\n")    
    for i in range (0,len(df)):
        if (df.model_id.loc[i] == 2):
            f.write(str(df.name.loc[i]))
            f.write(" ")

    f.write("\n\n")
    f.write("Non existent: ")
    f.write(str(count[2])) 
    f.write("\n")    
    for i in range (0,len(df)):
        if (df.model_id.loc[i] == 2):
            f.write(str(df.name.loc[i]))
            f.write(" ")

    f.write("\n\n")

def main():
    func = sys.argv[1] 
    if func == 'loggerstatus':
        global globaldf
        printloggerStatus()
        encodeDataFrame(globaldf)
    elif func == 'checknetstat':
        print 'check net stat'
        gsmStatus()
    elif func == 'otherstat':
        printotherStatus()
    else:
        print '>> Wrong argument'

if __name__ == "__main__":
    main()