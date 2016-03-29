# -*- coding: utf-8 -*-
"""
Created on Mon Feb 16 19:02:06 2015

@author: kennex
"""

import pandas
import sys
import ConfigParser
import numpy

ext = '.csv'

configFile = "main-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

section = "File I/O"
MachineFP = cfg.get(section,'MachineFP')
InputFP = MachineFP + cfg.get(section,'InputFP')
OutputFP = MachineFP + cfg.get(section,'OutputFP')
#ext = cfg.get(section,'extension')

section = "Data Settings"
col = cfg.get(section,'csvname')
colrawinput = col+".csv"
motor1 = cfg.get(section,'motor1')
motor2 = cfg.get(section,'motor2')

dfnames =  cfg.get(section,'csvformat').split(",")
df = pandas.read_csv(InputFP+colrawinput, names=dfnames)

df2 = df.copy()
df2 = df2.drop('batt',1)
df2 = df2.sort(['id','msgid'],ascending=[True,True])

#print df2
dfmag = numpy.sqrt(df2.x**2 + df2.y**2 + df2.z**2)

#dfmag2 = dfmag.copy()
#dfmag = pandas.rolling_mean(dfmag,12)
#dfmag  = pandas.rolling_mean(dfmag,24)

df2 = pandas.concat([df2,dfmag], axis=1)

df2.columns = ['ts','id','msgid','x','y','z','mag']

for i in pandas.unique(df2.id.ravel()):
        df_per_id = df2.copy()
        df_per_id = df_per_id[(df_per_id.id == i)]
        ### Min Max bit difference
        del_a1 = df_per_id[(df_per_id.msgid == 32)]
        del_a2 = df_per_id[(df_per_id.msgid == 33)]
        
        del_a1[['ts','x']].to_csv(OutputFP+str(i)+'_'+'x1'+ext, header=True,index=False)
        del_a1[['ts','y']].to_csv(OutputFP+str(i)+'_'+'y1'+ext, header=True,index=False)
        del_a1[['ts','z']].to_csv(OutputFP+str(i)+'_'+'z1'+ext, header=True,index=False)
        del_a1[['ts','mag']].to_csv(OutputFP+str(i)+'_'+'mag1'+ext, header=True,index=False)
        
        del_a2[['ts','x']].to_csv(OutputFP+str(i)+'_'+'x2'+ext, header=True,index=False)
        del_a2[['ts','y']].to_csv(OutputFP+str(i)+'_'+'y2'+ext, header=True,index=False)
        del_a2[['ts','z']].to_csv(OutputFP+str(i)+'_'+'z2'+ext, header=True,index=False)
        del_a2[['ts','mag']].to_csv(OutputFP+str(i)+'_'+'mag2'+ext, header=True,index=False)
