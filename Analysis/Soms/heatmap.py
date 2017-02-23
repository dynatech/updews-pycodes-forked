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
#t_timestamp='2016-03-01'
#for a in range(1,17,1):


# Commented these out since the 3 lines below should be on a different file
#   - Prado
#col = raw_input('column name: ').lower()
#t_timestamp = raw_input('target date (ex. 2017-01-01): ').lower()
#t_win = raw_input('select monitoring window[1d, 3d, 30d]: ').lower()

def heatmap(col, t_timestamp, t_win = '1d', is_debug = False):
	
	df_merge = pd.DataFrame()
	smin=0; smax=255;mini = 0; maxi = 1300
	
	
	if (t_win == '1d'):
		timew = 24
		interval = '30Min'
	elif (t_win == '3d'):
		timew = 72
		interval = '90Min'
	elif (t_win == '30d'):
		timew = 720
		interval = '1D'
	else:
		print "invalid monitoring window"
	
	
	t_timestamp = pd.to_datetime(pd.to_datetime(t_timestamp) + timedelta(hours = 24))
	f_timestamp = pd.to_datetime(pd.to_datetime(t_timestamp) - timedelta(hours = timew))	
	if (len(col)<=4):
		print "No calib data for V1"
	else:
		
	
		query = "select num_nodes from senslopedb.site_column_props where name = '%s'" %col
		
		node = qs.GetDBDataFrame(query)
		for node_num in range (1,int(node.num_nodes[0])+1):
			
			df = CSR.getsomscaldata(col,node_num,f_timestamp,t_timestamp)
			if (df.empty == True):
				return "No Soil Moisture Data"
			
				
			else:
				df = df.reset_index()
				df.ts=pd.to_datetime(df.ts)
			
				df.index=df.ts                         
				df.drop('ts', axis=1, inplace=True)    
			
				df=df[((df<1300) == True) & ((df>0)==True)] 
				df['cval'] = df['mval1'].apply(lambda x:(x- mini) * smax / (maxi) + smin)
				dfrs =pd.rolling_mean(df.resample(interval), window=3, min_periods=1)   #mean for one day (dataframe)
				if 'mval1' in df.columns:				
					dfrs = dfrs.drop('mval1', axis=1)
					pd.options.display.float_format = '{:,.0f}'.format
					
					n=len(dfrs)-1
				
					dfp=dfrs[n-timew:n]
					dfp = dfp.reset_index()
				
					df_merge = pd.concat([df_merge, dfp], axis = 0)
	
			
					df_merge['ts'] = df_merge.ts.astype(str)
					dfjson = df_merge.to_json(orient='records' , double_precision=0)
				else:
					print "no data"
				
				return dfjson
