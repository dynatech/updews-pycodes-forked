import os,time,re,sys
from datetime import datetime as dt
import senslopedbio as dbio
import messageprocesses as msgproc
import senslopeServer as server
import lockscript
import gsmSerialio as gsmio
import argparse
#---------------------------------------------------------------------------------------------------------------------------

def get_arguments():
    parser = argparse.ArgumentParser(description="Run SMS server [-options]")
    parser.add_argument("-i", "--instance", 
        help="db instance to read smsinbox from")
    parser.add_argument("-p", "--process_identifier", 
        help="process identifier")
    
    try:
        args = parser.parse_args()

        # if args.status == None:
        #     args.status = 0
        # if args.messagelimit == None:
        #     args.messagelimit = 200
        return args        
    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()

def main():

    args = get_arguments()

    if args.instance is None:
        instance = 'gsm'
    else:
        instance = args.instance

    if args.process_identifier is None:
        identifier = 'processmessages'
    else:
        identifier = args.process_identifier

    lockscript.get_lock(identifier)

    # force backup
    while True:
        print instance
        allmsgs = dbio.getAllSmsFromDb("UNREAD",instance)
        if len(allmsgs) > 0:
            msglist = []
            for item in allmsgs:
                smsItem = gsmio.sms(item[0], str(item[2]), str(item[3]), str(item[1]))
                msglist.append(smsItem)
            allmsgs = msglist

            read_success_list, read_fail_list = msgproc.ProcessAllMessages(allmsgs,"procfromdb",instance)

            dbio.setReadStatus("READ-SUCCESS",read_success_list,instance)
            dbio.setReadStatus("READ-FAIL",read_fail_list,instance)
            sleeptime = 5
        else:
            # sleeptime = 60
            # print '>> Checking for alert sms'
            # alertmsg = CheckAlertMessages()
            # if alertmsg:
            #     WriteOutboxMessageToDb(alertmsg,smartnumbers)
            #     WriteOutboxMessageToDb(alertmsg,globenumbers)
        
            server.logRuntimeStatus(identifier,"alive")
            print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
            return
            # time.sleep(sleeptime)

if __name__ == "__main__":
    main()
