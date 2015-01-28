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

# make a copy of the maindataframe
df = df[df.id==nid].drop('id',1)
dff = df.copy()

# adjust accelerometer values for valid overshoot ranges
dff.x[(dff.x<-2970) & (dff.x>-3072)] = dff.x[(dff.x<-2970) & (dff.x>-3072)] + 4096
dff.y[(dff.y<-2970) & (dff.y>-3072)] = dff.y[(dff.y<-2970) & (dff.y>-3072)] + 4096
dff.z[(dff.z<-2970) & (dff.z>-3072)] = dff.z[(dff.z<-2970) & (dff.z>-3072)] + 4096

# remove all invalid values and relabel as 'f' (filtered)
dff2 = dff.copy()
dff2.x[(dff2.x > 1126) | (dff2.x < 0)] = np.nan
dff2.y[(dff2.y > 1126) | (dff2.y < -1126)] = np.nan
dff2.z[(dff2.z > 1126) | (dff2.z < -1126)] = np.nan
dff2.m[(dff2.m > 4000) | (dff2.m < 2000)] = np.nan
dff2.columns = ['xf','yf','zf','mf']

# remove all non orthogonal values
dff2m = dff2[['xf','yf','zf']]/1024
mag = (dff2m.xf*dff2m.xf + dff2m.yf*dff2m.yf + dff2m.zf*dff2m.zf ).apply(np.sqrt)
lim = .09

# concatenate raw and filtered values
dfA = pd.concat([dff, dff2[['xf','yf','zf']][(mag>(1-lim)) & (mag<(1+lim))], dff2.mf], axis=1)

dfA = dfA.resample(interval, how='first')

ext = "-rf.csv"
nids = repr(nid)
dfA[['x','xf']].to_csv(OutputFP+col+nids+"x"+ext, header=False)
dfA[['y','yf']].to_csv(OutputFP+col+nids+"y"+ext, header=False)
dfA[['z','zf']].to_csv(OutputFP+col+nids+"z"+ext, header=False)
dfA[['m','mf']].to_csv(OutputFP+col+nids+"m"+ext, header=False)
