# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 14:55:55 2015

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
if (len(sys.argv)==2):
    col = sys.argv[1]
else:
    col = cfg.get(section,'ColName')

rainfile = col+'-rain.csv'
df = pd.read_csv(InputFP+rainfile, names=['ts','r'],parse_dates=[0]).set_index('ts')

#df = df.r.astype(float)
df = df.resample('15Min').fillna(0.00)

dfs = pd.rolling_sum(df,96)
dfa = pd.concat([df,dfs],axis=1)

dfa.columns = ['15Min','24Hr']

dfa.to_csv(OutputFP+col+"-rain.csv", header=True, float_format='%.2f') 