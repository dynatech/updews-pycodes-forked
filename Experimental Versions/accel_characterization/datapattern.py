# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 15:08:05 2015

@author: Mizpah
"""

import pandas as pd
import numpy as np
import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import ConfigParser

# Read config settings
configFile = "main-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

# Get file config settings
section = "File I/O"
MachineFP = cfg.get(section,'MachineFP')
InputFP = MachineFP + cfg.get(section,'InputFP')
OutputFP = MachineFP + cfg.get(section,'OutputFP')

# Get data config settings    
section = "Data Settings"

if (len(sys.argv)==3):
    col = sys.argv[1]
    nid = int(sys.argv[2])
    
    if sys.argv[2]=="all":
        nids = range(1,41)
    else:
        nids = int(sys.argv[2])
    
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

# remove all non orthogonal value
dff2m = dff2[['xf','yf','zf']]/1024
mag = (dff2m.xf*dff2m.xf + dff2m.yf*dff2m.yf + dff2m.zf*dff2m.zf).apply(np.sqrt)
lim = .09

#dffltr = pd.concat([dff2[['xf','yf','zf']][(mag>(1-lim)) & (mag<(1+lim))], dff2.mf], axis=1)
dffltr = pd.concat([dff2[['xf','yf','zf']][(mag>(1-lim)) & (mag<(1+lim))], dff2.mf])

# concatenate raw and filtered values
#dfA = pd.concat([dff, dffltr], axis=1)
dfA = pd.concat([dff, dffltr])

dfA = dfA.resample('30Min', how='first', fill_method = 'pad')

# get the mean and SD
dfstats = dffltr.copy()
dfstats = dfstats.resample('30Min', how='first', fill_method = 'pad')
dfmean = pd.stats.moments.rolling_mean(dfstats,48, min_periods=1, freq=None, center=False, how=None)
dfsd = pd.stats.moments.rolling_std(dfstats,48, min_periods=1, freq=None, center=False, how=None)
#dfsd=0

#debug
#dfulimits = dfmean
#dfulimits.columns = ['xu','yu','zu','mu']
#dfllimits = dfsd
#dfllimits.columns = ['xl','yl','zl','ml']


#setting of limits
dfulimits = dfmean + (3*dfsd)
dfulimits.columns = ['xu','yu','zu','mu']
dfllimits = dfmean - (3*dfsd)
dfllimits.columns = ['xl','yl','zl','ml']

dffinal = pd.concat([dfA, dfulimits, dfllimits], axis=1)

#removes row with no data
dffinal = dffinal[dffinal.x.notnull()]

dfoutlier = dffinal.copy()
dfoutlier.xf[(dfoutlier.xf > dfoutlier.xu) | (dfoutlier.xf < dfoutlier.xl)] = np.nan
dfoutlier.yf[(dfoutlier.yf > dfoutlier.yu) | (dfoutlier.yf < dfoutlier.yl)] = np.nan
dfoutlier.zf[(dfoutlier.zf > dfoutlier.zu) | (dfoutlier.zf < dfoutlier.zl)] = np.nan
#dfoutlier.mf[(dfoutlier.mf > dfoutlier.mu) | (dfoutlier.mf < dfoutlier.mf)] = np.nan

#dfoutlier = dfoutlier[dfoutlier.x.notnull()]
dfdata = dfoutlier[dfoutlier.x.notnull()]

#gets average every 30 days, 90 days, 180 days, 365 days
#problem: bakit may nafifilter
#sa x meron sa xf wala na
#dfdata30D = pd.stats.moments.rolling_mean(dfdata, 30*48, min_periods=1, freq=None, center=False, how='backfill')
#dfdata90D = pd.stats.moments.rolling_mean(dfdata, 90*48, min_periods=1, freq=None, center=False, how='backfill')
#dfdata180D = pd.stats.moments.rolling_mean(dfdata, 180*48, min_periods=1, freq=None, center=False, how='backfill')
#dfdata365D = pd.stats.moments.rolling_mean(dfdata, 365*48, min_periods=1, freq=None, center=False, how='backfill')

dfdata30D = dfdata.resample('30D', how = 'mean', fill_method = 'none')
dfdata30D.columns = ['30Dx', '30Dy', '30Dz', '30Dm', '30Dxf', '30Dyf', '30Dzf', '30Dmf', '30Dxu', '30Dyu', '30Dzu', '30Dmu', '30Dxl', '30Dyl', '30Dzl', '30Dml']
dfdata90D = dfdata.resample('90D', how = 'mean', fill_method = 'none')
dfdata90D.columns = ['90Dx', '90Dy', '90Dz', '90Dm', '90Dxf', '90Dyf', '90Dzf', '90Dmf', '90Dxu', '90Dyu', '90Dzu', '90Dmu', '90Dxl', '90Dyl', '90Dzl', '90Dml']
dfdata180D = dfdata.resample('180D', how = 'mean', fill_method = 'none')
dfdata180D.columns = ['180Dx', '180Dy', '180Dz', '180Dm', '180Dxf', '180Dyf', '180Dzf', '180Dmf', '180Dxu', '180Dyu', '180Dzu', '180Dmu', '180Dxl', '180Dyl', '180Dzl', '180Dml']
dfdata365D = dfdata.resample('365D', how = 'mean', fill_method = 'none')
dfdata365D.columns = ['365Dx', '365Dy', '365Dz', '365Dm', '365Dxf', '365Dyf', '365Dzf', '365Dmf', '365Dxu', '365Dyu', '365Dzu', '365Dmu', '365Dxl', '365Dyl', '365Dzl', '365Dml']

#dfdpat = pd.concat([dfdata30D, dfdata90D, dfdata180D, dfdata365D], axis = 1)
dfdpat = pd.concat([dfdata30D, dfdata90D, dfdata180D, dfdata365D])

#dfdataA = 
#dfdataB =
#dfdataC = 

#CODE ISSUE: nagfill ako sa mga no data, hindi ko maka-capture yung seasons na walang data
#on the other hand, kung trend lang din naman ung titingnan ko, ok lang
#gawa na lang ng another na itatapat para malaman kung kelan walang data
#so pwede ko i-push 'tong code na 'to para sa data pattern
#so pwede ako kumuha ng data pattern every 30 days, 90 days, 180 days, 365 days
#magnitude, x, y, z

#ok na pala ang rolling mean. so pagkakuha ng rolling mean, i-resample ko yung dataframe ko na kukunin lang niya ung
#every 30, every 60, every 180, every 365
#tapos un ung ipa-plot ko for data pattern

ext = ".csv"
nids = repr(nid)


#dfdata.to_csv(OutputFP+col+nids+"-dataset"+ext, header = True)
#dfdata30D.to_csv(OutputFP+col+nids+"-data30"+ext, header = True)
#dfdata90D.to_csv(OutputFP+col+nids+"-data90"+ext, header = True)
#dfdata180D.to_csv(OutputFP+col+nids+"-data180"+ext, header = True)
#dfdata365D.to_csv(OutputFP+col+nids+"-data365"+ext, header = True)

dfdpat.to_csv(OutputFP+col+nids+"-dpat"+ext, header = True)
dfdpat[['30Dxf','90Dxf','180Dxf','365Dxf']].to_csv(OutputFP+col+nids+"-x"+ext, header = True)
dfdpat[['30Dyf','90Dyf','180Dyf','365Dyf']].to_csv(OutputFP+col+nids+"-y"+ext, header = True)
dfdpat[['30Dzf','90Dzf','180Dzf','365Dzf']].to_csv(OutputFP+col+nids+"-z"+ext, header = True)

#dfdata30D[['30Dxf']].to_csv(OutputFP+col+nids+"-30Dx"+ext, header = True)
#dfdata30D[['30Dyf']].to_csv(OutputFP+col+nids+"-30Dy"+ext, header = True)
#dfdata30D[['30Dzf']].to_csv(OutputFP+col+nids+"-30Dz"+ext, header = True)

#dfdata90D[['90Dxf']].to_csv(OutputFP+col+nids+"-90Dx"+ext, header = True)
#dfdata90D[['90Dyf']].to_csv(OutputFP+col+nids+"-90Dy"+ext, header = True)
#dfdata90D[['90Dzf']].to_csv(OutputFP+col+nids+"-90Dz"+ext, header = True)

#dfdata180D[['180Dxf']].to_csv(OutputFP+col+nids+"-180Dx"+ext, header = True)
#dfdata180D[['180Dyf']].to_csv(OutputFP+col+nids+"-180Dy"+ext, header = True)
#dfdata180D[['180Dzf']].to_csv(OutputFP+col+nids+"-180Dz"+ext, header = True)

#dfdata365D[['365Dxf']].to_csv(OutputFP+col+nids+"-365Dx"+ext, header = True)
#dfdata365D[['365Dyf']].to_csv(OutputFP+col+nids+"-365Dy"+ext, header = True)
#dfdata365D[['365Dzf']].to_csv(OutputFP+col+nids+"-365Dz"+ext, header = True)
