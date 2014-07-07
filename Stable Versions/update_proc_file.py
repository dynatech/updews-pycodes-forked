##############################################################################
## This module reads and checks purged file and appends the processed file. ##
##############################################################################


import os
import csv
import math
import numpy as np
import scipy as sp
import scipy.optimize
import ConfigParser

from dateutil.parser import parse
from array import *
from datetime import date, timedelta
from datetime import datetime as dt
import time
import pandas as pd

class ColumnArray:
    def __init__(self, name, number_of_segments, segment_length):
        self.name = name
        self.nos = number_of_segments
        self.seglen = segment_length  

############################################################
##                   UPDATE FUNCTION                      ##
############################################################

def update_proc_file(col):
    
    inputfname=col.name+".csv"
    print "Updating ",inputfname+"..."

    dfo = pd.read_csv(PurgedFilePath+col.name+".csv", names=['ts','id','x','y','z','m'], index_col=0)

    # do the conversion
    dfx = col.seglen/np.sqrt(1+(dfo.z/(np.sqrt(dfo.x**2+dfo.y**2)))**2+(dfo.y/(np.sqrt(dfo.x**2+dfo.z**2)))**2)
    dfxz = dfx*(dfo.z/(np.sqrt(dfo.x**2+dfo.y**2)))
    dfxy = dfx*(dfo.y/(np.sqrt(dfo.x**2+dfo.z**2)))

    # append to a new dataframe
    df_proc = pd.concat([dfo.id, dfxz, dfxy, dfo.m], axis=1, keys=['id','xz','xy','m'])

    # print to csv
    df_proc.to_csv(ProcFilePath+col.name+"_proc.csv", float_format='%.4f',header=False)
    
    print "Update successful!\n"


######################################################################
##                         MAIN PROGRAM                             ##
######################################################################

##gets configuration from file##
cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')

MachineFilePath = cfg.get('File I/O','MachineFilePath')
PurgedFilePath = MachineFilePath + cfg.get('File I/O','PurgedFilePath')
ProcFilePath = MachineFilePath + cfg.get('File I/O','ProcFilePath')
ColumnPropertiesFile = cfg.get('File I/O','ColumnPropertiesFile')

try:

    # read from column properties file
    fo = csv.reader(open(ColumnPropertiesFile, 'r'),delimiter=',')

    # create list of site columns and make them class ColumnArray
    column_list = []
    for line in fo:
        col = ColumnArray(line[0], int(line[1]), float(line[2]))
        column_list.append(col)

    # main loop
    while True:
        for column in column_list:
            update_proc_file(column)

        # repeat for in the next exact 30 minute interval
        tm = dt.today()
        cur_sec = tm.minute*60 + tm.second
        interval = 30        
        sleep_tm = 0
        for i in range(0, 60*60+1, interval*60):
            if i > cur_sec:
                print i
                sleep_tm = i
                break
        print 'Sleep..',
        print sleep_tm - cur_sec
        time.sleep(sleep_tm - cur_sec)
        
except KeyboardInterrupt:
    print 'Keyboard interrrupt'
    raw_input("Enter anything to quit")
