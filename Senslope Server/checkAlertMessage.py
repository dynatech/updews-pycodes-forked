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
    logRuntimeStatus("alert","checked")

    print '>> Checking for alert sms'
    alertmsg = CheckAlertMessages()
    if alertmsg:
        WriteOutboxMessageToDb(alertmsg,smartnumbers)
        WriteOutboxMessageToDb(alertmsg,globenumbers)
    else:
        print '>> No alert msg read.'
        
if __name__ == "__main__":
    main()
