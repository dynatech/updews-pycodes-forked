# -*- coding: utf-8 -*-
"""
Created on Fri Apr 08 13:49:21 2016

@author: SKY
"""

import os
import sys


#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path

import pandas as pd
import ConvertSomsRaw as CSR
import querySenslopeDb as qs
from datetime import timedelta
#column = 'gaasb'
#tdate='2016-03-01'
#for a in range(1,17,1):


#def heatmap(column,fdate,tdate ):
df_merge = pd.DataFrame()
smin=0; smax=255;mini = 0; maxi = 1300

column = raw_input('column name: ').lower()
tdate = raw_input('target date (ex. 2017-01-01): ').lower()
window = raw_input('select monitoring window[1d, 3d, 30d]: ').lower()
if (window == '1d'):
	timew = 24
	interval = '30Min'
elif (window == '3d'):
	timew = 72
	interval = '90Min'
elif (window == '30d'):
	timew = 720
	interval = '1D'
else:
	print "invalid monitoring window"


tdate = pd.to_datetime(pd.to_datetime(tdate) + timedelta(hours = 24))
fdate = pd.to_datetime(pd.to_datetime(tdate) - timedelta(hours = timew))	

query = "select num_nodes from senslopedb.site_column_props where name = '%s'" %column
node = qs.GetDBDataFrame(query)
for node_num in range (1,int(node.num_nodes[0])+1):
	df = CSR.getsomscaldata(column,node_num,fdate,tdate)

	df = df.reset_index()
	df.ts=pd.to_datetime(df.ts)

	df.index=df.ts                         
	df.drop('ts', axis=1, inplace=True)    

	df=df[((df<1300) == True) & ((df>0)==True)] 
	df['mval1'] = df['mval1'].apply(lambda x:(x- mini) * smax / (maxi) + smin)
	dfrs =pd.rolling_mean(df.resample(interval), window=3, min_periods=1)   #mean for one day (dataframe)
	

	n=len(dfrs)-1

	dfp=dfrs[n-timew:n]
	dfp = dfp.reset_index()
	


	
	df_merge = pd.concat([df_merge, dfp], axis = 0)
	dfpr = dfp.transpose()




dfjson = df_merge.to_json(orient='records')

print dfjson
