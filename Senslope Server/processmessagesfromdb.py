import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import winsound
import emailer
from senslopedbio import *
from gsmSerialio import *
from groundMeasurements import *
import multiprocessing
import SomsServerParser as SSP
import math
from messageprocesses import *
#---------------------------------------------------------------------------------------------------------------------------

def updateSimNumTable(name,sim_num,date_activated):
    db, cur = SenslopeDBConnect()
    
    while True:
        try:
            query = """select sim_num from senslopedb.site_column_sim_nums
                where name = '%s' """ % (name)
        
            a = cur.execute(query)
            if a:
                out = cur.fetchall()
                if (sim_num == out[0][0]):
                    print ">> Number already in database", name, out[0][0]
                    return
                                    
                break
            else:
                print '>> Number not in database', sim_num
                return
                break
        except MySQLdb.OperationalError:
            print '1.',
            raise KeyboardInterrupt
    
    query = """INSERT INTO senslopedb.site_column_sim_nums
                (name,sim_num,date_activated)
                VALUES ('%s','%s','%s')""" %(name,sim_num,date_activated)

    commitToDb(query, 'updateSimNumTable')                
    
def logRuntimeStatus(script_name,status):
    if (status == 'alive'):
        ts = dt.today()
        roundmintoten = int(math.floor(ts.minute / 10.0)) * 10
        logtimestamp = "%d-%02d-%02d %02d:%02d:00" % (ts.year,ts.month,ts.day,ts.hour,roundmintoten)
    else:
        logtimestamp = dt.today().strftime("%Y-%m-%d %H:%M:00")
    
    print ">> Logging runtime '" + status + "' at " + logtimestamp 
    
    query = """insert ignore into senslopedb.runtimelog
                (timestamp,script_name,status)
                values ('%s','%s','%s')
                """ %(logtimestamp,script_name,status)
    
    commitToDb(query, 'logRuntimeStatus')
       
def timeToSendAlertMessage(ref_minute):
    current_minute = dt.now().minute
    if current_minute % AlertReportInterval != 0:
        return 0,ref_minute
    elif ref_minute == current_minute:
        return 0,ref_minute
    else:
        return 1,current_minute
    
def SendAlertEmail(network, serverstate):
    print "\n\n>> Attemptint to send routine emails.."
    sender = '1234dummymailer@gmail.com'
    sender_password = '1234dummy'
    receiver =['ggilbertluis@gmail.com', 'dynabeta@gmail.com']
	
    ## select if serial error if active server if inactive server
    if serverstate == 'active':
        subject = dt.today().strftime("ACTIVE " + network + " SERVER Notification as  of %A, %B %d, %Y, %X")
        active_message = '\nGood Day!\n\nYou received this email because ' + network + ' SERVER is still active!\nThanks!\n\n-' + network + ' Server\n'
    elif serverstate == 'serial':
        subject = dt.today().strftime(network + 'SERVER No Serial Notification  as  of %A, %B %d, %Y, %X')
        active_message = '\nGood Day!\n\nYou received this email because ' + network + ' SERVER is NOT connected to Serial Port!\nPlease fix me.\nThanks!\n\n-' + network + ' Server\n'
    elif serverstate == 'inactive':
        subject = dt.today().strftime(network + 'SERVER No Serial Notification  as  of %A, %B %d, %Y, %X')
        active_message = '\nGood Day!\n\nYou received this email because ' + network + ' SERVER is now INACTIVE!\\nPlease fix me.\nThanks!\n\n-' + network + ' Server\n'
	
    p = multiprocessing.Process(target=emailer.sendmessage, args=(sender,sender_password,receiver,sender,subject,active_message),name="sendingemail")
    p.start()
    time.sleep(60)
    # emailer.sendmessage(sender,sender_password,receiver,sender,subject,active_message)
    print ">> Sending email done.."
    
def SendAlertGsm(network,alertmsg):
    try:
        if network == 'GLOBE':    
            numlist = globenumbers.split(",")
        else:
            numlist = smartnumbers.split(",")
        # f = open(allalertsfile,'r')
        # alllines = f.read()
        # f.close()
        for n in numlist:
            sendMsg(alertmsg,n)
    except IndexError:
        print "Error sending all_alerts.txt"

def UnexpectedCharactersLog(msg, network):
    print ">> Error: Unexpected characters/s detected in ", msg.data
    f = open(unexpectedchardir+network+'Nonalphanumeric_errorlog.txt','a')
    f.write(msg.dt + ',' + msg.simnum + ',' + msg.data+ '\n')
    f.close()

def WriteRawSmsToDb(msglist):
    createTable('smsinbox','smsinbox')
    createTable('smsoutbox','smsoutbox')
    
    query = "INSERT INTO smsinbox (timestamp,sim_num,sms_msg,read_status) VALUES "
    
    for m in msglist:
        query += "('" + str(m.dt.replace("/","-")) + "','" + str(m.simnum) + "','" + str(m.data) + "','UNREAD'),"
    
    query = query[:-1]
    
    commitToDb(query, "getAllSms")
        
def main():
            
    createTable("runtimelog","runtime")
    logRuntimeStatus("procfromdb","startup")

    # force backup
    while True:
        allmsgs = getAllSmsFromDb("UNREAD")
        if len(allmsgs) > 0:
            msglist = []
            for item in allmsgs:
                smsItem = sms(item[0], str(item[2]), str(item[3]), str(item[1]))
                msglist.append(smsItem)

        
            allmsgs = msglist
            read_success_list, read_fail_list = ProcessAllMessages(allmsgs,"procfromdb")

            setReadStatus("READ-SUCCESS",read_success_list)
            setReadStatus("READ-FAIL",read_fail_list)
        
        logRuntimeStatus("procfromdb","alive")
        print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
        time.sleep(5)
        
        
""" Global variables"""
checkIfActive = True
anomalysave = ''

cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

FileInput = cfg.getboolean('I/O','fileinput')
InputFile = cfg.get('I/O','inputfile')
ConsoleOutput = cfg.getboolean('I/O','consoleoutput')
DeleteAfterRead = cfg.getboolean('I/O','deleteafterread')
SaveToFile = cfg.getboolean('I/O','savetofile')
WriteToDB = cfg.getboolean('I/O','writetodb')
readfrom = cfg.getboolean('I/O','readfromdb')

# gsm = serial.Serial() 
Baudrate = cfg.getint('Serial', 'Baudrate')
Timeout = cfg.getint('Serial', 'Timeout')
Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')
SleepPeriod = cfg.getint('Misc','SleepPeriod')

# SMS Alerts for columns
##    Numbers = cfg.get('SMSAlert','Numbers')

SMSAlertEnable = cfg.getboolean('SMSAlert','Enable')
Directory = cfg.get('SMSAlert','Directory')
CSVInputFile = cfg.get('SMSAlert','CSVInputFile')
AlertFlags = cfg.get('SMSAlert','AlertFlags')
AlertReportInterval = cfg.getint('SMSAlert','AlertReportInterval')
smsgndfile = cfg.get('SMSAlert','SMSgndmeasfile')
gndmeasfilesdir= cfg.get('SMSAlert','gndmeasfilesdir')

##SMS alert numbers
smartnumbers = cfg.get('SMSAlert', 'smartnumbers')
globenumbers = cfg.get('SMSAlert', 'globenumbers')

successen = cfg.get('ReplyMessages','SuccessEN')

inboxdir = cfg.get('FileIO','inboxdir')
unknownsenderfile = cfg.get('FileIO','unknownsenderfile')
allalertsfile = cfg.get('FileIO','allalertsfile')

if __name__ == "__main__":
    print 'hey'
    main()
