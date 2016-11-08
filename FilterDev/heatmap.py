# -*- coding: utf-8 -*-
"""
Created on Fri Apr 08 13:49:21 2016

@author: SENSLOPEY
"""

#import matplotlib.pyplot as plt
#import numpy as np
#import pandas as pd
#column_labels = list('ABCD')
#row_labels = list('WXYZ')
#df=pd.read_csv('C:/Users/AnnaKatrina/Documents/SENSLOPE/FILTERS/tester/output/laysa.csv')
#plt.pcolor(df[0:10])
#data = np.random.rand(4,4)
#fig, ax = plt.subplots()
#heatmap = ax.pcolor(data, cmap=plt.cm.Blues)
#
## put the major ticks at the middle of each cell
#ax.set_xticks(np.arange(data.shape[0])+0.5, minor=False)
#ax.set_yticks(np.arange(data.shape[1])+0.5, minor=False)
#
## want a more natural, table-like display
#ax.invert_yaxis()
#ax.xaxis.tick_top()
#
#ax.set_xticklabels(row_labels, minor=False)
#ax.set_yticklabels(column_labels, minor=False)
#plt.show()


import pandas as pd
import ConvertSomsRaw as CSR
import querySenslopeDb as qs


#nodecount=['20','20','20','20', '15','16','10','10','20','15','18','13','15','15','15','12','22']

columns = ['nagsa','baysb','agbsb','mcasb', 
           'lpasa','lpasb','laysa','laysb','imesb',
           'barsc','mngsa','carsb','gaasa',
           'gaasb','hinsa','hinsb','talsa' ]

column = "laysa"
fdate='2013-01-1'
tdate='2016-08-1'
#for a in range(1,17,1):


#def heatmap(column,fdate,tdate ):
df_merge = pd.DataFrame()


days = 30
query = "select num_nodes from senslopedb.site_column_props where name = '%s'" %column
node = qs.GetDBDataFrame(query)
for node_num in range (1,int(node.num_nodes[0])):
	df = CSR.getsomscaldata(column,node_num,fdate,tdate)
#    df=pd.read_csv('C:/Users/JosephRyan/Desktop/SENSLOPE/FILTERS/tester/output/'+ str(columns[a])+'_CAL.csv')
#	print df
	df = df.reset_index()
	df.ts=pd.to_datetime(df.ts)
#df.drop('ts', axis=1, inplace=True)
	df.index=df.ts                         
	df.drop('ts', axis=1, inplace=True)    
#pd.to_datetime(df.index)
#df=df[((df<5000) == True) & ((df>2000)==True)]
	df=df[((df<1300) == True) & ((df>0)==True)]  
	dfrs =pd.rolling_mean(df.resample('1D'), window=3, min_periods=1)   #mean for one day (dataframe)
	dfrs.rename(columns={'mval1':node_num}, inplace=True)
	
#wmean=pd.ewma(df,span=48,min_periods=1)
#ncols=range(1,int(nodecount[6])+1)
#wmean.columns=[str(x) for x in ncols]
#n=len(wmean)
#dfp=wmean[n-48:n].transpose()

#number of days
#ploooooots
	n=len(dfrs)-1

	dfp=dfrs[n-days:n]
	dfp = dfp.reset_index()
	dfpr = dfp.transpose()
#	


#	df_merge = pd.concat([dfp, df_merge],axis = node_num)
	
#	dfpj = dfp.to_json(orient='records')

	df_merge = pd.concat([df_merge, dfpr], axis = 0)

#	df_merge = df_merge + dfpj



dfjson = df_merge.to_json(orient='records')
#
print dfjson
#return df_merge
		





#
#ax = plt.imshow(dfp,interpolation='nearest', cmap='summer_r',vmin=0, vmax=1500, extent=[0, days, int(nodecount[6]), 0]).axes
#plt.colorbar()
#ax.set_xticks(np.linspace(0, days-1, days))
#ax.set_yticks(np.linspace(1, int(nodecount[6])-1, int(nodecount[6])))
#ax.set_xticklabels(dfp.columns.date)
#y = np.linspace(int(0),10,1)
##plt.tight_layout()
#plt.xticks(rotation=90)
#ax.set_yticklabels(y)
#
#ax.set_title(str(columns[6])+' Calibrated')
#eeeennnddd
    #first week
#    n=len(dfrs)
#    dfp1=dfrs[0:1].transpose()
#    ax = plt.imshow(dfp1, interpolation='nearest', cmap='BrBG', extent=[0, 31, int(nodecount[a]), 0]).axes
#    ax.set_xticks(np.linspace(0, 31, 1))
#    ax.set_yticks(np.linspace(0, int(nodecount[a])-1, int(nodecount[a])))
#    ax.set_xticklabels(dfp1.columns.date)
#    plt.xticks(rotation=90)
#    ax.set_yticklabels(dfp1.index)
    
    
#    plt.savefig(str(columns[a])+'_CAL.png')
#    plt.close()

#ax.set_yticks([1,2,3,4,5,6,7,8,9,10])
#plt.figure(2)
#plt.imshow(v, interpolation='nearest', cmap='Oranges').axes