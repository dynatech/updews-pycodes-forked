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
#site = 'imesb'
#tdate='2016-03-01 08:00'
#days = '30d'
#is_debug = True
#for a in range(1,17,1):


# Commented these out since the 3 lines below should be on a different file
#   - Prado
#col = raw_input('column name: ').lower()
#t_timestamp = raw_input('target date (ex. 2017-01-01): ').lower()
#t_win = raw_input('select monitoring window[1d, 3d, 30d]: ').lower()

def heatmap(col, t_timestamp, t_win = '1d'):
	
	df_merge = pd.DataFrame()
	smin=0; smax=255;mini = 0; maxi = 1300
	
	
	if (t_win == '1d'):
		for_base = 0         
		timew = 24
		interval = '30T'
	elif (t_win == '3d'):
		for_base = 0      
		timew = 72
		interval = '120T'
	elif (t_win == '30d'):
		for_base = int(t_timestamp[11]+t_timestamp[12])  
		timew = 720
		interval = '24H'
	else:
		print "invalid monitoring window"
	
	f_timestamp = pd.to_datetime(pd.to_datetime(t_timestamp) - timedelta(hours = timew))	
	t_timestamp = pd.to_datetime(pd.to_datetime(t_timestamp) + timedelta(minutes = 30))
	
    
	
         
	if(len(col)>4):
		
	
		query = "select num_nodes from senslopedb.site_column_props where name = '%s'" %col
		
		node = qs.GetDBDataFrame(query)
		for node_num in range (1,int(node.num_nodes[0])+1):
			
			df = CSR.getsomscaldata(col,node_num,f_timestamp,t_timestamp, if_multi = True)
			
			if (df.empty == False):
				df = df.reset_index()
				df.ts=pd.to_datetime(df.ts)
			
				df.index=df.ts                         
				df.drop('ts', axis=1, inplace=True)    
			
				df=df[((df<1300) == True) & ((df>0)==True)] 
				df['cval'] = df['mval1'].apply(lambda x:(x- mini) * smax / (maxi) + smin)
				dfrs =pd.rolling_mean(df.resample(interval, base= for_base), window=3, min_periods=1)   #mean for one day (dataframe)
			
				if 'mval1' in df.columns:				
					dfrs = dfrs.drop('mval1', axis=1)
	
		
#				n=len(dfrs)-1
				dfrs = dfrs.reset_index(0)
			
#				dfp=dfrs[n-timew:n]
#				dfp = dfp.reset_index()
			
				df_merge = pd.concat([df_merge, dfrs], axis = 0)
				df_merge['ts'] = df_merge.ts.astype(object).astype(str)
				    
		
		
		dfjson = df_merge.to_json(orient='records' , double_precision=0)
		print dfjson
	else:
           return 'v1'                     
				
	


site = sys.argv[1]
tdate = sys.argv[2].replace("T"," ").replace("%20"," ")
days = sys.argv[3]
	

heatmap(site, tdate, t_win = days)