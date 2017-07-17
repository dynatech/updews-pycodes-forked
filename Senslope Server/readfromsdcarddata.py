import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
from senslopedbio import *
from groundMeasurements import *
import multiprocessing
import SomsServerParser as SSP
import messageprocesses as proc
from os import listdir
from os.path import isfile, join
#---------------------------------------------------------------------------------------------------------------------------

def main():
    # get the directory from the command line
    dirname = sys.argv[1]
    if not os.path.isdir(dirname):
        raise ValueError("Error: '" + dirname + "' does not exist")
    else:
        print ">> Listing files from '" + dirname
    
    files = [f for f in listdir(dirname) if isfile(join(dirname, f))]
    
    linecount = 1
    for fname in files:
        try:
            f = open(dirname + '\\' + fname, 'r')
        except IOError:
            f = open(dirname + '/' + fname, 'r')
        alllines = f.readlines()
        f.close()
        accel_dlist = []
        soms_dlist = []
        # sys.stdout = os.devnull
        # sys.stderr = os.devnull
        
        for line in alllines:
            print "Line:", linecount
            dlist = []
            try:
                dlist = proc.ProcTwoAccelColData(line,"","")
            except ValueError:
                print "Moving to different line"
            if dlist:
                if len(dlist[0][0]) == 6:
                    for item in dlist:
                        soms_dlist.append(item)
                else:
                    for item in dlist:
                        accel_dlist.append(item)
            linecount += 1
        # sys.stdout = sys.__stdout__
        # sys.stderr = sys.__stderr__
        # print "hey"
        if soms_dlist:
            proc.WriteSomsDataToDb(soms_dlist,"")
        if accel_dlist:
            proc.WriteTwoAccelDataToDb(accel_dlist,"")
            

if __name__ == "__main__":
    main()


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

unexpectedchardir = cfg.get('FileIO','unexpectedchardir')
inboxdir = cfg.get('FileIO','inboxdir')
unknownsenderfile = cfg.get('FileIO','unknownsenderfile')
allalertsfile = cfg.get('FileIO','allalertsfile')

