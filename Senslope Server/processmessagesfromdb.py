import os,time,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import emailer
from senslopedbio import *
from gsmSerialio import *
from groundMeasurements import *
import multiprocessing
import SomsServerParser as SSP
import math
from messageprocesses import *
from senslopeServer import *
#---------------------------------------------------------------------------------------------------------------------------

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
            sleeptime = 5
        else:
            sleeptime = 60
            # print '>> Checking for alert sms'
            # alertmsg = CheckAlertMessages()
            # if alertmsg:
            #     WriteOutboxMessageToDb(alertmsg,smartnumbers)
            #     WriteOutboxMessageToDb(alertmsg,globenumbers)
        
        logRuntimeStatus("procfromdb","alive")
        print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
        time.sleep(sleeptime)

if __name__ == "__main__":
    main()
