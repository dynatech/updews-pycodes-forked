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
df= 0

def readPbook():
    db, cur = dbio.SenslopeDBConnect('local') 
    query = """SELECT * FROM senslopedb.site_column_sim_nums """    
    df = psql.read_sql(query,db)
    return df


def checkCaseBasedFromInbox(df):
    db, cur = dbio.SenslopeDBConnect('gsm')
	
    timenow = 0 #not yet finished; may use a function to get current time
    
    for i in range (0, len(df)):
        s_name= str(df.name.loc[i]).lower()
        s_number= str(df.sim_num.loc[i])
        
        query = "SELECT sms_msg from senslopedb.smsinbox where sms_id > (select max(sms_id)-10000 from senslopedb.smsinbox) and sim_num = " + s_number + """ and (sms_msg like "%NO DATA PARSED%" or sms_msg like "%NO DATA FROM SENSELOPE%" or sms_msg like "%MANUAL RESET%") order by sms_id desc"""
        
        mesdf = psql.read_sql(query,db)
        tempStr= str(mesdf)
        
        if (tempStr.find("no data parsed"))> 0:
            case= "CASE 1"
            print "no data parsed: "
            print s_name 
        elif (tempStr.find("NO DATA FROM SENSELOPE"))> 0:
            print "no data from senselope: "
            print s_name
            case= "CASE 2"
        elif (tempStr.find("MANUAL RESET"))> 0:
            case= "CASE 3"
            print "manual reset: "
            print s_name  
        else:
            case= 'CONFIRM NETWORK STAT'   
        encodeCase("0", s_name, case)

        # try using the find fxn in a
        #find manual reset, no data etc 

def encodeCase(timestamp, lgr_name, case):
    db, cur = dbio.SenslopeDBConnect('local') 
    dbio.createTable("dlhealth", "dlhealth") 
    query = '''SELECT health_case from senslopedb.dlhealth where lgr_name = "''' + lgr_name + '''" order by case_id desc limit 1'''
    casedf = psql.read_sql(query,db)
    print casedf
	
    try:
        prevState = str(casedf.health_case.loc[0])
    except KeyError,e:
        prevState= 'new case'
    
    if ( prevState == case):
        print "yey!"
        print casedf.health_case.loc[0]
        print "="
        print case
    else:
        query = "INSERT into senslopedb.dlhealth(health_case, lgr_name, timestamp,updated_ts) values ('%s','%s','%s', '%s')" % (case,lgr_name,timestamp, timestamp)            
        print query
        dbio.commitToDb(query, 'dlhealth')

        #para sa isang side

    # if (a== same lang sa case): 
		#double check this query
		# query = """insert into senslopedb.dlhealth
  #           (updated_ts, lgr_name,health_case)
  #           values ('%s','%s','%s')
  #           on DUPLICATE key update
  #           updated_ts = '%s',
  #           lgr_name = '%s',
  #           health_case = '%s'""" %(txtdatetime,name,sim_num,msg,txtdatetime,sim_num,msg)
            
    # else:
    # 	query = """insert into senslopedb.dlhealth(updated_ts,lgr_name,case) values (%s','%s','%s')""" % (timestamp,lgr_name, case)            

    # dbio.commitToDb(query, 'dlhealth')

# def checkCaseBasedFromData():
	
# 	numberOfLoggers= len(df)
# 	query= """select lgr_name from senslopedb.dlhealth where case_id > (select max(case_id)-"""+ numberOfLoggers +""" from senslopedb.smsinbox) and 
# 		health_case = "CONFIRM NET STAT" order by case_id desc limit """ + numberOfLoggers
# 	#ang ginagawa niya ay yung mas recent na case 

# 	cur.execute(query)
# 	a= cur.fetchall

# 	save yung mga  


# 	i-save sa df yung mga sites na "confirm network status"
# 	call sites ng mga nasa df lang
# 	loop depende sa status
# 	encodeCase()

def callSite():
	df= 0
	return df

#print 'testing semi phonebook'
#readPbook()
print 'checkCaseBasedFromInbox'
checkCaseBasedFromInbox(readPbook())