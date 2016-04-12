# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 10:07:56 2015

@author: kennex
"""

import pandas
import sys
import ConfigParser
import numpy

from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import AdaBoostClassifier

#n_estimators = 400
#learning_rate = 1.

#clf = DecisionTreeClassifier(max_depth=9, min_samples_leaf=1)
#clf = AdaBoostClassifier(
#    base_estimator=dt_stump,
#    learning_rate=learning_rate,
#    n_estimators=n_estimators,
#    algorithm="SAMME")
clf = KNeighborsClassifier()
#clf.set_params(algorithm = 'ball_tree',n_neighbors=5, weights='distance')

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
df = pandas.read_csv(InputFP+colrawinput, names=dfnames,index_col=False)


dfout = pandas.read_csv(InputFP+testinput, names=testnames)
dftest = dfout.copy()
dfout = dfout.sort(['id','msgid'],ascending=[True,True])

df2 = df.copy()
df2 = df2.sort(['id','msgid'],ascending=[True,True])


#normalize training data
df2['x'] = (df2['x']/df2['x'].max(axis=1))*(1024)
df2['y'] = (df2['y']/df2['x'].max(axis=1))*(1024)
df2['z'] = (df2['z']/df2['x'].max(axis=1))*(1024)

#dfout = dfout.drop('ts',1)
#dfout['x'] = (dfout['x']/dfout['x'].max(axis=1))*(1024)
#dfout['y'] = (dfout['y']/dfout['x'].max(axis=1))*(1024)
#dfout['z'] = (dfout['z']/dfout['x'].max(axis=1))*(1024)


X = numpy.asarray(df2[['x','y','z']])
y = numpy.asarray(df2[['event']])
y = y.ravel()

#X_new = numpy.asarray(dfout[['x','y','z']])

clf = clf.fit(X,y)

#dfout = pandas.concat([dfout,pandas.DataFrame(clf.predict(X_new))], axis = 1)
#dfout = pandas.concat([dftest['ts'],dfout], axis=1)
#dfout.columns = ['ts','id','msgid','x','y','z','batt','event']

for i in pandas.unique(dfout.id.ravel()):
        df_per_id = dfout.copy()
        df_per_id = df_per_id[df_per_id.id==i]
        ### Min Max bit difference
        del_a1 = pandas.DataFrame(df_per_id[df_per_id.msgid==32])
        del_a1 = del_a1.reset_index(drop=True)
        del_a1['x'] = (del_a1['x']/del_a1['x'].max(axis=1))*(1024)
        del_a1['y'] = (del_a1['y']/del_a1['x'].max(axis=1))*(1024)
        del_a1['z'] = (del_a1['z']/del_a1['x'].max(axis=1))*(1024)        
        
        del_a2 = pandas.DataFrame(df_per_id[df_per_id.msgid==33])
        del_a2 = del_a2.reset_index(drop=True)
        del_a2['x'] = (del_a2['x']/del_a2['x'].max(axis=1))*(1024)
        del_a2['y'] = (del_a2['y']/del_a2['x'].max(axis=1))*(1024)
        del_a2['z'] = (del_a2['z']/del_a2['x'].max(axis=1))*(1024)   
        
        #X_new = numpy.asarray(del_a1[['x','y','z']])
        del_a1 = pandas.concat([del_a1,pandas.DataFrame(clf.predict(numpy.asarray(del_a1[['x','y','z']])))], axis = 1)
        del_a1.columns = ['ts','id','msgid','x','y','z','batt','event']
        #del_a1 = pandas.concat([del_a1,pandas.DataFrame(clf.predict_proba(numpy.asarray(del_a1[['x','y','z']])))], axis = 1)
        #del_a1.columns = ['ts','id','msgid','x','y','z','batt','event','prob']
        del_a1[['ts','x']].to_csv(OutputFP+str(i)+'_'+'x1'+ext, header=True,index=False)
        del_a1[['ts','y']].to_csv(OutputFP+str(i)+'_'+'y1'+ext, header=True,index=False)
        del_a1[['ts','z']].to_csv(OutputFP+str(i)+'_'+'z1'+ext, header=True,index=False)
        del_a1[['ts','event']].to_csv(OutputFP+str(i)+'_'+'event1'+ext, header=True,index=False)
        
        #X_new = numpy.asarray(del_a2[['x','y','z']])
        del_a2 = pandas.concat([del_a2,pandas.DataFrame(clf.predict(numpy.asarray(del_a2[['x','y','z']])))], axis = 1)
        del_a2.columns = ['ts','id','msgid','x','y','z','batt','event']
        #del_a2 = pandas.concat([del_a2,pandas.DataFrame(clf.predict_proba(numpy.asarray(del_a2[['x','y','z']])))], axis = 1)
        #del_a2.columns = ['ts','id','msgid','x','y','z','batt','event','prob']
        del_a2[['ts','x']].to_csv(OutputFP+str(i)+'_'+'x2'+ext, header=True,index=False)
        del_a2[['ts','y']].to_csv(OutputFP+str(i)+'_'+'y2'+ext, header=True,index=False)
        del_a2[['ts','z']].to_csv(OutputFP+str(i)+'_'+'z2'+ext, header=True,index=False)
        del_a2[['ts','event']].to_csv(OutputFP+str(i)+'_'+'event2'+ext, header=True,index=False)

