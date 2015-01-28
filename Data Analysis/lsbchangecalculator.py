# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 16:43:37 2015

@author: chocolate server
"""

import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import ConfigParser


configFile = "main-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

section = "File I/O"
MachineFP = cfg.get(section,'MachineFP')
InputFP = MachineFP + cfg.get(section,'InputFP')
OutputFP = MachineFP + cfg.get(section,'OutputFP')
    
section = "Data Settings"
if (len(sys.argv)==3):
    col = sys.argv[1]
    nid = int(sys.argv[2])
else:
    col = cfg.get(section,'ColName')
    nid = cfg.getint(section,'Node')
colrawinput = col+"-raw.csv"
dateStart = dt.strptime(cfg.get(section,'DateStart'),"%m/%d/%Y")
dateEnd = dt.strptime(cfg.get(section,'DateEnd'),"%m/%d/%Y")
interval = cfg.get(section,'Interval')

dfnames =  cfg.get(section,'csvformat').split(",")
df = pd.read_csv(InputFP+colrawinput, names=dfnames,parse_dates=[0])
df = df.set_index(['ts'])
df = df[df.id==nid]
df = df.resample(interval).fillna(method='pad')
df = df[['x','y','z']]

if (interval=='1D'):
    mult = 1
elif (interval=='30Min'):
    mult = 48
dflsbA = df - df.shift(3*mult)
dflsbB = df - df.shift(7*mult)
dflsbC = df - df.shift(14*mult)

ext = ".csv"
nids = repr(nid)

dflsbA.to_csv(OutputFP+col+nids+"-lsb3"+ext, header=False)
dflsbB.to_csv(OutputFP+col+nids+"-lsb7"+ext, header=False)
dflsbC.to_csv(OutputFP+col+nids+"-lsb14"+ext, header=False)
