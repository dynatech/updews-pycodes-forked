import os,time,re,sys
from datetime import datetime as dt
import senslopedbio as dbio
import messageprocesses as msgproc
import senslopeServer as server
import lockscript
import gsmSerialio as gsmio
#---------------------------------------------------------------------------------------------------------------------------

def main():

    lockscript.get_lock('processmessages')
            
    # dbio.createTable("runtimelog","runtime")
    # logRuntimeStatus("procfromdb","startup")

    # force backup
    while True:
        allmsgs = dbio.getAllSmsFromDb("UNREAD")
        if len(allmsgs) > 0:
            msglist = []
            for item in allmsgs:
                smsItem = gsmio.sms(item[0], str(item[2]), str(item[3]), str(item[1]))
                msglist.append(smsItem)
            allmsgs = msglist

            read_success_list, read_fail_list = msgproc.ProcessAllMessages(allmsgs,"procfromdb")

            dbio.setReadStatus("READ-SUCCESS",read_success_list)
            dbio.setReadStatus("READ-FAIL",read_fail_list)
            sleeptime = 5
        else:
            # sleeptime = 60
            # print '>> Checking for alert sms'
            # alertmsg = CheckAlertMessages()
            # if alertmsg:
            #     WriteOutboxMessageToDb(alertmsg,smartnumbers)
            #     WriteOutboxMessageToDb(alertmsg,globenumbers)
        
            server.logRuntimeStatus("procfromdb","alive")
            print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
            return
            # time.sleep(sleeptime)

if __name__ == "__main__":
    main()
