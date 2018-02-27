import os,time,serial,re,sys,traceback
import MySQLdb, subprocess
from datetime import datetime as dt
from datetime import timedelta as td
import severdbio as dbio
import somsparser as SSP
import gsmserver as server
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

def read_dataframe():
    
    localdf=0
    db, cur = dbio.db_connect('local')
    
    query= '''SELECT DISTINCT logger_health.health_id, ltr.logger_id, health, battery, signal_strength, ibx_errors, data_presence, network, logger_name, model_id, ts \
        from (select logger_health.health_id from logger_health where health_id IN (select max(logger_health.health_id) from logger_health group by logger_id)) a \
        inner join logger_health on logger_health.health_id = a.health_id \
        inner join loggers on logger_health.logger_id= loggers.logger_id  \
        inner join (SELECT logger_id, ts from logger_mobile inner join lastTextReceived  on logger_mobile.mobile_id = lastTextReceived.mobile_id where logger_mobile.mobile_id in (select max(logger_mobile.mobile_id) from senslopedb.logger_mobile group by logger_id)) ltr \
        on ltr.logger_id = logger_health.logger_id \
        where logger_health.data_presence !=5 '''

    try:
        localdf = psql.read_sql(query,db)
        print 'localdf'
        print localdf
    except pd.io.sql.DatabaseError,e:
        print e
        print 'nag error:'
        localdf = 0
    print localdf

    return localdf

def data_presence(): 
    global globaldf
    globaldf= read_dataframe() 
    print globaldf
    timeNow= dt.today()
    print len(globaldf)
    for i in range (0, len(globaldf)):
        lgr_name= str (globaldf.logger_name.loc[i])
        logger_id= int(globaldf.logger_id.loc[i])
        logger_model= int(globaldf.model_id.loc[i])
        data_presence = int(globaldf.data_presence.loc[i])
        if (data_presence != 5):        
            
            if (logger_model > 26 and logger_model < 35): #gateway
                lgr_name = "rain_" + lgr_name
            else:
                lgr_name = "tilt_"+ lgr_name

            res= check_last_data(lgr_name)

            if (res != None):
                encode_case(timeNow, logger_id, res)
            else: 
                encode_case(timeNow, logger_id, 4)
    globaldf= read_dataframe() 

def check_last_data(lgr_name):
    db, cur = dbio.db_connect('local')
    query = "SELECT ts FROM " + lgr_name + " order by data_id desc limit 1"
    print query
    timeNow= dt.today()
    try:    
        a = cur.execute(query)
        if a:
            out = cur.fetchall()         
            st=out[0]
            lastTime= st[0]
            tdelta  =timeNow- lastTime  

            if lastTime > timeNow:   
                return 1
            elif tdelta.seconds/60 < 59:
                return 1
            elif tdelta.days < 2 and tdelta.seconds/60  > 60: 
                return 2 #intermittent
            else:
                return 3 #for maintenance

    except MySQLdb.ProgrammingError:     
        return 50  
    db.close()

def loggerHealth():
    # returns kung ano na nga ba yung status ng mga loggers (kung nagtetext lang)
    global globaldf
    
    timeNow= dt.today()
    for i in range(0, len(globaldf)):
        data_presence = int (globaldf.data_presence.loc[i])
        logger_id= int(globaldf.logger_id.loc[i])
        if (data_presence != 1 or data_presence!= 5):
            lastTime= str(globaldf.ts.loc[i])
            lastTime= dt.strptime(lastTime, "%Y-%m-%d %H:%M:%S")
            tdelta= timeNow- lastTime
            
            print globaldf.logger_id[0]
            print lastTime
            
            if lastTime > timeNow:   
                print "active"
                health= 1
                # return 1
            elif tdelta.seconds/60 < 59:
                print "active"
                health= 1
                # return 1
            elif tdelta.days < 2 and tdelta.seconds/60  > 60: 
                print "intermittent"
                health= 2
                # return 2 #intermittent
            else:
                print "for maintenance"
                health= 3
                # return 3 #for maintenance
                # lagay sa 

    # update na dapat yung status nilaaaaa
            globaldf.set_value(i, 'health', health) 
                   
        # globaldf.set_value(i, 'case', health)     

def check_ibx_errors(): #okay
    global globaldf
    db, cur = dbio.db_connect('local')

    # <baka may mali sa pag query dito: hindi kapareho ng sa logger sa labas query>
    print globaldf

    query= '''SELECT sms_msg, logger_health.logger_id, health from logger_mobile \
        inner join  (SELECT * FROM senslopedb.smsinbox_loggers where read_status = (-1) \
        and (inbox_id > ((select max(inbox_id) from smsinbox_loggers) -5000)) order by inbox_id) a \
        on a.mobile_id= logger_mobile.mobile_id  \
        inner join logger_health on logger_health.logger_id = logger_mobile.logger_id \
        where data_presence != 1 and data_presence != 5 \
        order by logger_id'''     
    
    try:
        ibxdf = psql.read_sql(query,db)
        print ibxdf

        for i in range(0, len(ibxdf)):
            logger_id= int(ibxdf.logger_id.loc[i]) 
            tempStr= ibxdf.sms_msg.loc[i]            
            tempStr = tempStr.lower()

            print str(logger_id)

            if (tempStr.find("no data parsed"))>= 0:
                case= 2    
            elif (tempStr.find("nodataparsed"))>= 0:
                case = 2
            elif (tempStr.find("no data from senselope"))>= 0:
                case= 3
            elif (tempStr.find("nodatafromsenselope"))>= 0:
                case=3 
            elif (tempStr.find("nodatafromsenslope"))>= 0:
                case = 3
            elif (tempStr.find("manual reset"))>= 0:
                case= 4
            elif (tempStr.find("manualreset"))>= 0:
                case= 4
            elif (tempStr[0] == '*'):
                case= 6
            else:
                case = 5 #trash di pa okay or error in parsing!
            
            print case

            globaldf.set_value(i, 'ibx_errors', case) 
        # gawa ng bagong dataframe na nakalagay yung chenes
        # mag for loop para malaman yunng i na same yung case
                # globaldf.set_value(i, 'case', health)        
    except pd.io.sql.DatabaseError,e:
        print e
        ibxdf = 0

    db.close()
        
def store_health_data(): #---------------------same lang naman dapat to---------------------------------------------
    global globaldf 

    db, cur = dbio.db_connect('local')    
    columns= ['logger_id','batv1', 'batv2', 'signal', 'model']
    siteHealthdf = pd.DataFrame(columns=columns)

    for i in range (0, len(globaldf)):
        if (globaldf.health.loc[i] != 1 and globaldf.health.loc[i] != 5):
            lgr_name= str(globaldf.logger_name.loc[i])
            logger_id= int(globaldf.logger_id.loc[i])
            logger_model= int(globaldf.model_id[i])

            if (logger_model > 1 and logger_model < 10): #arq
                query = '''SELECT avg(battery1), avg(battery2), avg(csq) FROM rain_''' + lgr_name + ''' order by data_id desc limit 48'''
                print query
                #average lang ito, dapat sana trendin, pero next time na powsz
                try:
                    tempdf = psql.read_sql(query,db)

                    siteHealthdf.set_value(i, 'batv1', tempdf.loc[0, 'avg(battery1)'])
                    siteHealthdf.set_value(i, 'batv2', tempdf.loc[0, 'avg(battery2)'])
                    siteHealthdf.set_value(i, 'signal', tempdf.loc[0, 'avg(csq)'])
                    
                except pd.io.sql.DatabaseError,e:
                   tempdf= 0
            
            elif (logger_model > 9 and logger_model < 35 ): #version 3
                sitecode= lgr_name[:3]    

                query = '''SELECT avg(battery1),avg(csq) from rain_''' + lgr_name + ''' order by data_id desc limit 48'''
       

                try:
                    tempdf = psql.read_sql(query,db)

                    siteHealthdf.set_value(i, 'batv1', tempdf.loc[0, 'avg(battery1)'])
                    siteHealthdf.set_value(i, 'signal', tempdf.loc[0, 'avg(csq)'])
                except pd.io.sql.DatabaseError,e:
                    tempdf = 0
    
            siteHealthdf.set_value(i, 'model', logger_model)
            siteHealthdf.set_value(i, 'logger_id', logger_id)
    return siteHealthdf

def generate_health_case(siteHealthdf):
    global globaldf
    print siteHealthdf  

    for i in range (0, len(globaldf)):

        try:
            logger_model = int (siteHealthdf.model.loc[i])
            try:
                batv1= float(siteHealthdf.batv1.loc[i])
            except:
                batv1 = (siteHealthdf.batv1.loc[i])
            print batv1

            try:
                batv2= float(siteHealthdf.batv2.loc[i])
            except:
                batv2 = (siteHealthdf.batv2.loc[i])

            try:
                signal= float(siteHealthdf.signal.loc[i])
            except:
                signal = (siteHealthdf.signal.loc[i])
            
            logger_id= str(siteHealthdf.logger_id.loc[i])
            logger_model= int(siteHealthdf.model.loc[i])
            
            bhealth=0
            shealth=0
            
            print "logger_id" + logger_id

            if (logger_model > 1 and logger_model < 10): #arq
                if (batv1 < 3.3 and batv2 < 3.3):
                    bhealth = 2
                if (signal < 10):
                    shealth = 3

            if (logger_model > 9 and logger_model < 18): #router
                if (batv1< 13.0):
                    bhealth= 3
                if (signal > 90 ):
                    shealth = 2
            
            if (logger_model > 17 and logger_model < 27): #regular    (walang case ng mababang csq dito)
                if (batv1 < 13):
                    bhealth = 3
                if (signal < 10.0):
                    shealth = 3

            if (logger_model > 26 and logger_model < 35): #gateway
                if (batv1 < 13.0):
                    bhealth = 3
                if (signal < 10):
                    shealth = 3
            globaldf.set_value(i, 'battery', int(bhealth)) 
            globaldf.set_value(i , 'signal_strength', int(shealth)) 
            # globaldf.set_value(i, 'health_case', health)
        except KeyError, e:
            print ""  
    # return 0
    print globaldf
    encode_dataframe()

def gsm_status(): #ibaaaaang approach
    timeNow= dt.today()
    lgrdf= read_dataframe()

    # global lgrdf 
    
    gsmio.init_gsm('call')
    for i in range (0,len(lgrdf)):
        if (lgrdf.health_case.loc[i] == 4): #3 na yung chinecheck
            s_number= str(lgrdf.sim_num.loc[i])
            s_id= int(lgrdf.logger_id.loc[i])
            t= time.strftime('%Y-%m-%d %H:%M:%S')
            s_number= s_number.replace("63","0",1)

            if (int(lgrdf.model_id.loc[i])>9 and int(lgrdf.model_id.loc[i])<18): #kapag router, wag na tawagan (?)
                print "not calling routers"
                health= 35
                print health
            else:

                q= gsmio.call_site(s_number).replace("\r\n","")        
                if (q== "NO CARRIER"):
                    health= 31
                elif (q== "BUSY" ):
                    health= 32
                elif(q== "NO ANSWER"):
                    health= 33
                elif(q== "NO DIALTONE"):
                    health= 34
                else:
                    health= 4
            lgrdf.set_value(i, 'health_case', health)  
            encode_case(timeNow, s_id, health)




def checkLastV1soms(lgr_name):
    db, cur = dbio.db_connect('local')
    query = """select timestamp, mvalue from """ + lgr_name + """ order by timestamp desc limit 1 """
    
    timeNow= dt.today()
    try:    
        a = cur.execute(query)
        if a:
            out = cur.fetchall()         
            st=out[0]
            lastTime= st[0]
            tdelta  =timeNow- st[0]  

            if (st[1]!= 0):
                if lastTime > timeNow:   
                    return 1
                elif tdelta.seconds/60 < 59:
                    return 1
                elif tdelta.days < 2 and tdelta.seconds/60  > 60:
                    return 2
                else:
                    return 3
            else:
                return 3

    except MySQLdb.ProgrammingError:     
        return 50  
    db.close()

def encode_case(timestamp, logger_id, case): #may provision kung data_presence lang ba o health na
    db, cur = dbio.db_connect('local')
    query = '''SELECT * from logger_health where logger_id = ''' + str(logger_id) + ''' order by health_id desc limit 1'''
    
    casedf = psql.read_sql(query,db)

    try:

        prevState = str(casedf.data_presence.loc[0])
        prevCaseId = str(casedf.health_id.loc[0])
        print "logger_id " + str(logger_id)
        print "prevState " + str(prevState) 
        print "case " + str(case)
    
    except KeyError,e:
        prevState= 0
    # try:
    if ( int(prevState) == int(case)):
        query = """UPDATE logger_health SET ts_updated ='""" +str(timestamp)+ """' WHERE health_id=""" + prevCaseId
    else:
        query = "INSERT into logger_health (data_presence, logger_id, ts_initial, ts_updated, battery, health, signal_strength, network, ibx_errors) values ('%d','%d','%s', '%s', '%s', '%s', '%s', '%s', '%s')" %(case, logger_id, timestamp, timestamp, str(casedf.battery.loc[0]), str(casedf.health.loc[0]), str(casedf.signal_strength.loc[0]), str(casedf.network.loc[0]), str(casedf.ibx_errors.loc[0])) 
    dbio.commit_to_db(query, 'logger_health')
    # except ValueError, e:
    #         query = "INSERT into logger_health (data_presence, logger_id, ts_initial, ts_updated) values (6,'%d','%s', '%s')" %( logger_id, timestamp, timestamp)

def update_dead_sites():
    db, cur = dbio.db_connect('local') 
    
    query= "SELECT logger_id FROM senslopedb.loggers where date_deactivated IS NOT NULL"
    lgrdf = psql.read_sql(query,db)
    
    timeNow =  dt.today()
        
    for i in range (0, len(lgrdf)):    
        logger_id= str(lgrdf.logger_id.loc[i])
        health= 5
        encode_case(timeNow, lgrdf.logger_id.loc[i], health)

def print_logger_status():
    # {yung mga nasa logger}

    globaldf.sort_values(['name'], ascending=[True],inplace = True)
    globaldf.index = range(0,len(globaldf))

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
        if (globaldf.health.loc[i] == 1): #active sites 
            f.write(str(globaldf.name.loc[i]))
            f.write(" ")

    for i in range (0,len(globaldf)):
        if (globaldf.health.loc[i] == 2): #intermittent sites 
            f.write(str(globaldf.name.loc[i]))
            f.write(" ")

    for i in range (0,len(globaldf)):
        if (globaldf.health.loc[i] == 3): #for maintenance sites 
            f.write(str(globaldf.name.loc[i]))
            f.write(" ")
    count= globaldf["health_case"].value_counts()

    # for 

    for j in range(2,36):
        try:
            co= count[j]
            f.write('\n')
            f.write('\n')
            
            title= "For Maintenance Loggers (Status: " + str(j) +"): " + str(co) + "\n" 
            # print title
            f.write(title)
            print j
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

def encode_dataframe():
    timeNow= dt.today()
    global globaldf

    for i in range (0, len(globaldf)):
        he= globaldf.health.loc[i]
        batt= globaldf.battery.loc[i]
        sig= globaldf.signal_strength.loc[i]
        ibx= globaldf.ibx_errors.loc[i]
        h_id= globaldf.health_id.loc[i]

        query= 'UPDATE logger_health SET ts_updated = "{}" , health= {}, battery = {}, signal_strength={}, ibx_errors= {} where health_id = {}'.format(timeNow, he, batt, sig, ibx, h_id)
        dbio.commit_to_db(query, 'logger_health')
        print "dbio commit to query"

def update_csv():
    global globaldf
    # mondir= c.fileio.monitoringoutputdir
    mondir =  "/home/dewsl/zheyserver/"
    f= open(mondir+ "loggerStatus.csv", "w");
     
    globaldf.to_csv(mondir+ "loggerStatus.csv", index=False)
    # globaldf.to_csv("loggerStatus.csv", index=False)



    # with open(mondir+ "loggerStatus.csv") as f:
    #     reader = csv.DictReader(f)
    #     rows = list(reader)

    # with open('test.json', 'w') as f:
    #     json.dump(rows, f)

def check_active_rain():
    #rain_tsm
    
    raindf=0
    
    db, cur = dbio.db_connect('local')
    query= '''SELECT name, loggers.model_id from loggers inner join (select model_id from logger_models where has_rain =1) a on a.model_id= loggers.model_id'''
    try:
        raindf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        raindf = 0

    for i in range (0, len(raindf)):
        lgr_name= str (raindf.name.loc[i])
        logger_model= int(raindf.model_id.loc[i])
        
        if (logger_model <35 and logger_model > 26):    
            lgr_name = lgr_name[:3] + "w"
            print lgr_name
        else:
            lgr_name= lgr_name + 'w'
        
        res= check_last_active(lgr_name)

        raindf.set_value(i, 'model_id', res)    
    return raindf

def activeSoms():
    somsdf=0
    db, cur = dbio.db_connect('local')
    query= '''SELECT version, name, loggers.model_id from loggers inner join (select model_id from logger_models where has_soms =1) a on a.model_id= loggers.model_id  inner join (select column_id, version from columns) b on loggers.column_id = b.column_id'''
    
    try:
        somsdf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        somsdf = 0

    for i in range (0, len(somsdf)):
        lgr_name= str (somsdf.name.loc[i])
        if (somsdf.version.loc[i] != 1):
            lgr_name= lgr_name + 'm'
            res= check_last_active(lgr_name)
        else:
            print lgr_name
            res= checkLastV1soms(lgr_name)

        somsdf.set_value(i, 'model_id', res)  
    return somsdf

def check_active_piezo():
    piezodf=0
    db, cur = dbio.db_connect('local')
    query= '''SELECT name, loggers.model_id from loggers inner join (select model_id from logger_models where has_piezo =1) a on a.model_id= loggers.model_id'''
    
    try:
        piezodf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
        piezodf = 0

    for i in range (0, len(piezodf)):
        lgr_name= str (piezodf.name.loc[i])
        lgr_name= lgr_name + 'pz'
        res= check_last_active(lgr_name)
        if (res == 50):
            lgr_name= lgr_name + 'pz'  
            res= check_last_active(lgr_name)
            
        piezodf.set_value(i, 'model_id', res)    
   
    return piezodf


def print_other_status():
    timeNow =  dt.today()
    
    raindf= check_active_rain()
    pzdf= check_active_piezo()
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
    write_status(f, "Rain", raindf)
    f.write("\n\n")
    write_status(f, "Piezo", pzdf) 
    f.write("\n\n")
    write_status(f, "Soil Moisture", somsdf) 
    

    f = open(mondir+"otherSensors_health.txt", "r");
    for line in f.readlines():
        print line

def write_status(f, sensortype, df):

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
    f.write(str(count[30])) 
    f.write("\n")    
    for i in range (0,len(df)):
        if (df.model_id.loc[i] == 30):
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
    f.write(str(count[50])) 
    f.write("\n")    
    for i in range (0,len(df)):
        if (df.model_id.loc[i] == 50):
            f.write(str(df.name.loc[i]))
            f.write(" ")

    f.write("\n\n")

def manual_change():
    logger = raw_input('Enter logger name: ')
    
    db, cur = dbio.db_connect('local')
    query = """SELECT logger_health.logger_id, health_id, health_case from logger_health inner join loggers on logger_health.logger_id= loggers.logger_id where loggers.name= '""" + logger + """' and  logger_health.health_id IN (select max(logger_health.health_id) from logger_health group by logger_id) """
    # print query
    timeNow= dt.today()
    try:    
        a = cur.execute(query)
        if a:
            out = cur.fetchall()         
            st=out[0]
            prn= logger + "'s previous state is case " + str(st[2])
            print prn
            prevCaseId= st[1]

            try:
                case = int(input('Enter new case number: '))
                que= "You entered " + str(case) + ". Are you sure you want to update the status of " +logger + " from " + str(st[2]) + " to case " + str(case) + "? [y/n] \n"
                ans= raw_input(que)
                if (ans == 'y'):         
                    query = """UPDATE logger_health SET ts_updated = '""" +str(timeNow)+ """' , health_case= """ + str(case) + " WHERE health_id= " + str(prevCaseId)
                    dbio.commit_to_db(query, 'logger_health')
                    print "entry updated"

            except NameError:
                print "Not a valid case. Enter an integer"

        else:
            err= logger + " is not a logger"
            print err
    except MySQLdb.ProgrammingError:     
        err= logger + " is not a logger"
        print err 
    
    db.close()

def main():
    func = sys.argv[1] 
    
    if func == 'loggerstatus':
        update_dead_sites()
        global globaldf
        globaldf= read_dataframe()
        print globaldf 
        # print ('-----------------------data_presence()')
        
        # data_presence()
        # print ('-----------------------loggerHealth()')
        
        # loggerHealth()
        print ('-----------------------check_ibx_errors()')
        
        check_ibx_errors()
        # generate_health_case(store_health_data())  
        # print globaldf
        # update_csv()
        # print 'csvUpdate'

    elif func == 'checknetstat':
        print 'check net stat'
        update_dead_sites()
        gsm_status()
    elif func == 'otherstat':
        print_other_status()
    elif func == 'manual':
        manual_change()
    else:
        print '>> Wrong argument'

if __name__ == "__main__":
    main()