# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 10:07:56 2015

@author: kennex
"""

import pandas
import sys
import ConfigParser
import numpy

#from sklearn.svm import LinearSVC
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import SGDClassifier
#from sklearn.neighbors import NeighborsClassifier
from sklearn.svm import LinearSVC
#clf = LinearSVC()
clf2 = LogisticRegression()
clf3 = SGDClassifier()
clf4 = LinearSVC()

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
testcsv =cfg.get(section,'testcsv')
col = cfg.get(section,'csvname')
colrawinput = col+".csv"
testinput = testcsv+".csv"

motor1 = cfg.get(section,'motor1')
motor2 = cfg.get(section,'motor2')
col = cfg.get(section,'csvname')

dfnames =  cfg.get(section,'csvformat').split(",")
testnames = cfg.get(section,'testformat').split(",")
df = pandas.read_csv(InputFP+colrawinput, names=dfnames)


#dfout = pandas.read_csv(InputFP+testinput, names=testnames)
dfout = df.copy()
dfout = dfout.drop('event',1)
dftest = dfout.copy()


df2 = df.copy()
df2 = df2.drop('batt',1)
df2 = df2.drop('ts',1)
df2 = df2.sort(['id','msgid'],ascending=[True,True])
#df2 = df2.drop('id',1)
#df2 = df2.drop('msgid',1)

#norm1 = df2.max(axis=1)

#normalize training data
df2['x'] = (df2['x']/df2['x'].max(axis=1))*(1024)
df2['y'] = (df2['y']/df2['x'].max(axis=1))*(1024)
df2['z'] = (df2['z']/df2['x'].max(axis=1))*(1024)

#print [df2['x'].max(axis=1)]

df3 = df2.copy()

df2 = df2.drop('event',1)

Xdf3 = df3.drop('id',1)
df3 = df3.drop('msgid',1)
df3 = df3.drop('x',1)
df3 = df3.drop('y',1)
df3 = df3.drop('z',1)


dfout = dfout.drop('batt',1)
dfout = dfout.drop('ts',1)
dfout['x'] = (dfout['x']/dfout['x'].max(axis=1))*(1024)
dfout['y'] = (dfout['y']/dfout['x'].max(axis=1))*(1024)
dfout['z'] = (dfout['z']/dfout['x'].max(axis=1))*(1024)


X = numpy.asarray(df2)
y = numpy.asarray(df3)
y = y.ravel()

X_new = numpy.asarray(dfout[['x','y','z']])

clf3 = clf3.fit(X,y)

dfout = pandas.concat([dfout,pandas.DataFrame(clf3.predict(X_new))], axis = 1)
dfout = pandas.concat([dftest['ts'],dfout], axis=1)
dfout.columns = ['ts','id','msgid','x','y','z','event']

for i in pandas.unique(df2.id.ravel()):
        df_per_id = dfout.copy()
        df_per_id = df_per_id[(df_per_id.id == i)]
        ### Min Max bit difference
        del_a1 = df_per_id[(df_per_id.msgid == 32)]
        del_a2 = df_per_id[(df_per_id.msgid == 33)]
      
#        del_a1 = pandas.concat([del_a1,df2['x']], axis = 1)
        del_a1[['ts','x']].to_csv(OutputFP+str(i)+'_'+'x1'+ext, header=True,index=False)
#        del_a1 = pandas.concat([del_a1,df2['y']], axis = 1)
        del_a1[['ts','x']].to_csv(OutputFP+str(i)+'_'+'y1'+ext, header=True,index=False)
#        del_a1 = pandas.concat([del_a1,df2['z']], axis = 1)
        del_a1[['ts','z']].to_csv(OutputFP+str(i)+'_'+'z1'+ext, header=True,index=False)
#        #del_a1 = pandas.concat([del_a1,df2['event']], axis = 1)
        del_a1[['ts','event']].to_csv(OutputFP+str(i)+'_'+'event1'+ext, header=True,index=False)

#        del_a2 = pandas.concat([del_a2,df2['x']], axis = 1)
        del_a2[['ts','x']].to_csv(OutputFP+str(i)+'_'+'x2'+ext, header=True,index=False)
#        del_a2 = pandas.concat([del_a2,df2['y']], axis = 1)
        del_a2[['ts','y']].to_csv(OutputFP+str(i)+'_'+'y2'+ext, header=True,index=False)
#        del_a2 = pandas.concat([del_a2,df2['z']], axis = 1)
        del_a2[['ts','z']].to_csv(OutputFP+str(i)+'_'+'z2'+ext, header=True,index=False)
        #del_a2 = pandas.concat([del_a2,df2['event']], axis = 1)
        del_a2[['ts','event']].to_csv(OutputFP+str(i)+'_'+'event2'+ext, header=True,index=False)

