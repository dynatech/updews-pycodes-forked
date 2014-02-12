# Col_Nod_Alerts.py
# Author: Rick Bahague
# Email: rick@opensourceshoppe.com
# For Senselope Project
# All Rights Reserved
# sample data: 2013-04-06 13:24:00,1,986,-22,-12,1,-0.7,-1.3,241.4,1.5,2548,1
# Time, Node_Number, x, y, z, good_data, xz, xy, phi, rho, moisture, moisture_filter

import os
from array import *
import pandas as pd
import csv
import math
import numpy as np
import scipy as sp
import scipy.optimize
from datetime import datetime, date, time, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator, AutoMinorLocator
from matplotlib import transforms as mtransforms
from matplotlib import font_manager as font
from matplotlib.figure import Figure #as mFig
from matplotlib.axis import Axis
from matplotlib.backend_bases import FigureCanvasBase
from matplotlib.backend_bases import NavigationToolbar2 as Nav

from scipy.interpolate import UnivariateSpline
import scipy.stats.stats as st
import sys
import ricsat

#Global settings
INPUT_FILE_PATH = "../csv/"
OUTPUT_FILE_PATH = "../FiguresForUpload/"
NODES_TO_CHECK = 5
TVELA1 = 0.005 #m/day
TVELA2 = 0.5 #m/day
TTILT  = 0.05 #M
OP_AXIS_K = 0.1
ADJ_NODE_K = 0.5

def getSensorData(filename, fine):
	#Sample: 2013-04-06 13:24:00,15,998,-61,-10,1,-0.6,-3.5,260.7,3.5,2343,1
	#col_name = ["Time", "Node", "x", "y", "z", "good_data", "xz", "xy", "phi", "rho", "moisture", "moisture_filter"]
	#col_type = ['object', "int", "float", "float", "float", "int", "float", "float", "float", "float", "float", "float"]

	if(fine == 1):
		col_name = ['id', 'subdays_fine_node', 'subtilt_fine', 'subdtilt_fine', 'sensor_fine', 'subdays_fine']
	else:
		col_name = ['id', 'sensor', 'subdays', 'subdays_node', 'sensor', 'subtilt']

	try:
		#data = np.genfromtxt(INPUT_FILE_PATH + filename, delimiter = ",", names = col_name, converters = {'Time': convertTime})
		#data = pd.read_csv(INPUT_FILE_PATH + filename, header=None, names=col_name, parse_dates=True)
		data = pd.read_csv(INPUT_FILE_PATH + filename, index_col = 0)
	except IOError:
		print "Data not found for " + filename
		data = 0

	return data

def toDataFrame(subtilt, subdays, subtilt_fine, subdtilt_fine, subdays_fine,spline,INPUT_which_node, axes, input_file_name):
  subdays_fine_node_series = pd.Series(INPUT_which_node)
  subtilt_fine_series = pd.Series(subtilt_fine)
  subdtilt_fine_series = pd.Series(subdtilt_fine)
  sensor_fine_series = pd.Series(input_file_name)
  subdays_fine_series = pd.Series(subdays_fine)

  subtilt_series = pd.Series(subtilt)
  subdays_node_series = pd.Series(INPUT_which_node)
  sensor_series = pd.Series(input_file_name)
  subdays_series = pd.Series(subdays)

  spline_series = pd.Series(spline,index=subdays_fine)

  dataframe_fine = {'subdays_fine_node': subdays_fine_node_series, 'subtilt_fine':subtilt_fine_series,'subdtilt_fine':subdtilt_fine_series,  'sensor_fine': sensor_fine_series, 'subdays_fine': subdays_fine_series}

  dataframe = {'subtilt':subtilt_series, 'subdays_node': subdays_node_series, 'sensor': sensor_series, 'subdays': subdays_series}

  return pd.DataFrame(dataframe), pd.DataFrame(dataframe_fine)

def dataframeToCSV(dataframe,filename):
	path = INPUT_FILE_PATH + filename
	dataframe.to_csv(path)

def writericalert(out,filename):
	#[cur_node+1,nodealert,round(xztilt[-1]-xztilt[0],2),round(xzvel[-1],3),round(xytilt[-1]-xztilt[0],2),round(xyvel[-1],3)]
	with open(INPUT_FILE_PATH + filename , 'a') as csvfile:
		alertwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
		alertwriter.writerow(out)

def getSensorConstants():
	data_file=("eeet","sinb_purged","sint_purged","sinu_purged","lipb_purged","lipt_purged","bolb_purged","pugb_purged","pugt_purged","mamb_purged","mamt_purged","oslt_purged","oslb_purged","labt_purged", "labb_purged", "gamt_purged","gamb_purged", "humt_purged","humb_purged", "plat_purged","plab_purged","blct_purged","blcb_purged")
	sensor_nodes=(14,29,19,29,28,31,30,14,10,29,24,21,23,39,25,18,22,21,26,39,40,24,19)
	col=(0.5,1,1,1,0.5,0.5,0.5,1.2,1.2,1.0,1.0,1.,1.,1.,1.,1.,1.,1.,1,0.5,0.5,1,1)	#source of seg_len

	return data_file, sensor_nodes, col

def getNodeAlerts(xy_node,xz_node):
	if(len(xy_node.values) == 0):
		node_alert = ['No data', 0, 0, 0, 0, 0, 0]
		return node_alert

	xy_last_tilt = xy_node.ix[max(xy_node.index),'subtilt_fine']
	xz_last_tilt = xz_node.ix[max(xz_node.index),'subtilt_fine']
	xy_first_tilt = xy_node.ix[min(xy_node.index),'subtilt_fine']
	xz_first_tilt = xz_node.ix[min(xz_node.index),'subtilt_fine']

	xy_last_vel = xy_node.ix[max(xy_node.index),'subdtilt_fine']
	xz_last_vel = xz_node.ix[max(xz_node.index),'subdtilt_fine']
	xy_first_vel = xy_node.ix[min(xy_node.index),'subdtilt_fine']
	xz_first_vel = xz_node.ix[min(xz_node.index),'subdtilt_fine']

	#print xy_last_tilt, xz_last_tilt, xz_first_tilt, xy_first_tilt

	#set alert constants:
	Tvela1 = TVELA1 #m/day
	Tvela2 = TVELA2 #m/day
	Ttilt = TTILT   #m
	op_axis_k = OP_AXIS_K
	adj_node_k = ADJ_NODE_K
	nodealert = 0

	if(abs(xz_last_vel) >= abs(xy_last_vel)):
		if(abs(xz_last_vel) > Tvela1):
			if((abs(xz_last_tilt) - xz_first_tilt) <= Ttilt):
				nodealter = 0
			else:
				if(abs(xz_last_vel) <= Tvela2):
					if(abs(xy_last_vel) > op_axis_k * abs(xz_last_vel)): #can disable op_axis_k
						nodealert = 1
					else:
						nodealert = 0
				else:
					if(abs(xy_last_vel) >= op_axis_k*abs(xz_last_vel)):
						nodealert = 2
					else:
						nodealert = 0
		else:
			nodealert = 0
	else:
		if(abs(xy_last_vel) > Tvela1):
			if(abs(xy_last_tilt - xy_first_tilt) <= Ttilt):
				nodealert = 0
			else:
				if(abs(xy_last_vel) <= Tvela2):
					if(abs(xz_last_vel) > op_axis_k * abs(xy_last_vel)):
						nodealert = 1
					else:
						nodealert = 0
				else:
					if(abs(xz_last_vel) > op_axis_k * abs(xy_last_vel)):
						nodealert = 2
					else:
						nodealert = 0
		else:
			nodealert = 0

	node_alert = [xz_node.ix[min(xz_node.index),'sensor_fine'], xz_node.ix[min(xz_node.index),'subdays_fine_node'], nodealert, (xz_last_tilt - xz_first_tilt), xz_last_vel, (xy_last_tilt - xz_first_tilt), xy_last_vel]

	return node_alert


def dataFrameChecks(dataframe_xz, dataframe_xy, dataframe_fine_xz, dataframe_fine_xy):
  print 'xz head: \n'
  print dataframe_xz.head()
  print 'xy head: \n'
  print dataframe_xy.head()
  print 'xz fine head: \n'
  print dataframe_fine_xz.head()
  print 'xy fine head: \n'
  print dataframe_fine_xy.head()
  print 'dataframe shapes: \n'
  print dataframe_xz.shape, dataframe_xy.shape, dataframe_fine_xy.shape, dataframe_fine_xz.shape
  print 'dataframe_xz description: \n'
  print dataframe_xz.describe()
  print 'dataframe_xy description: \n'
  print dataframe_xy.describe()
  print 'dataframe_fine_xz description: \n'
  print dataframe_fine_xz.describe()
  print 'dataframe_fine_xy description: \n'
  print dataframe_fine_xy.describe()
  print 'xz tail: \n'
  print dataframe_xz.tail()
  print 'xy tail: \n'
  print dataframe_xy.tail()
  print 'xz fine tail: \n'
  print dataframe_fine_xz.tail()
  print 'xy tail: \n'
  print dataframe_fine_xy.tail()

def loadNodeAlerts():
	dtype_col = "S50,i8,i8,f8,f8,f8,f8"
	#dtype_col = {'node': np.float64, 'node_alert': np.float64, 'xz_last - xz_firts tilt': np.float64, 'xzvel': np.float64, 'xy-xz tilt': np.float64, 'xyvel': np.float64 }
	col_name = ['sensor', 'node', 'node_alert', 'xz_last - xz_firts tilt', 'xzvel', 'xy-xz tilt', 'xyvel' ]
	try:
		#data = pd.read_csv(INPUT_FILE_PATH + 'node_alerts.csv', names = col_name)
		data_temp = np.genfromtxt(INPUT_FILE_PATH + 'node_alerts.csv',delimiter=',', dtype = dtype_col, names = col_name) #can be changed to NodeAlerts.csv if wants to compute using Col_Nod_Alerts.py node alert calc
		data = pd.DataFrame.from_records(data_temp)
	except IOError:
		print "Data not found for " + filename
		data = 0
	return data

def setNodeAlert(now_date):
	#get data from ricsat.py
	if(isinstance(getSensorData('xz.csv', 0), pd.DataFrame) == False):
		print 'ricsat sensor data processing....'
		writericalert(['node','nodealert','xzlast-xzfirst tilt','xzvel_last','xylast-xzfirst tilt','xyvel last','sensor'],'ricsatalerts.csv')
		dataframe_xz, dataframe_xy, dataframe_fine_xz, dataframe_fine_xy = ricsat.GeneratePlots(now_date)
		#store dataframe to csv
		dataframeToCSV(dataframe_xz,'xz.csv')
		dataframeToCSV(dataframe_xy,'xy.csv')
		dataframeToCSV(dataframe_fine_xz,'xz_fine.csv')
		dataframeToCSV(dataframe_fine_xy,'xy_fine.csv')
		print 'ricsat sensor data generated.'
	else:
		dataframe_xz = getSensorData('xz.csv', 0)
		dataframe_xy = getSensorData('xy.csv', 0)
		dataframe_fine_xz = getSensorData('xz_fine.csv', 1)
		dataframe_fine_xy = getSensorData('xy_fine.csv', 1)

	data_file, sensor_nodes, col = getSensorConstants()
	alert_prev = 0
	alert_node = ['sensor', 'node', 'nodealert', 'xz_last_xz_first_tilt', 'xz_last_vel', 'xy_last_xz_first_tilt', 'xy_last_vel']
	#writericalert(alert_node,'node_alerts.csv') #writes labels to csv

	for f in data_file:
		sensor = f + '_proc_chunked.csv'
		s_xz = dataframe_fine_xz[dataframe_fine_xz.sensor_fine == sensor]
		s_xy = dataframe_fine_xy[dataframe_fine_xy.sensor_fine == sensor]
		max_nodes = s_xz.subdays_fine_node.max()
		if(max_nodes > 0):
			for i in range(0,max_nodes+1):
				xz_node =  s_xz[s_xz.subdays_fine_node == i]
				xy_node =  s_xy[s_xy.subdays_fine_node == i]
				alert_node = getNodeAlerts(xy_node,xz_node)
				writericalert(alert_node,'node_alerts.csv') #writes values to csv
				alert_current = alert_node[2]
	return

def checkValidColumnAlert(i,data_slice,check,nodes_to_check,max_nodes):
	a_nodes = data_slice['node'].values.tolist()
	check_nodes = range(1,nodes_to_check + 1)
	alert1 = 0
	alert2 = 0
	node = np.NaN

	for s in check_nodes:
		n = abs(s-i)
		n2 = abs(s+i)

		try:
			a = a_nodes.index(n)
			temp = data_slice[data_slice.node == n]
			test1 = max(temp[['xzvel','xyvel']].values.tolist()[0])
		except ValueError:
			test1 = 0

		try:
			a = a_nodes.index(n2)
			temp2 = data_slice[data_slice.node == n2]
			test2 = max(temp2[['xzvel','xyvel']].values.tolist()[0])
		except ValueError:
			test2 = 0

		if(test1 >= check*1.0/2**s):
			alert1 = 1
			node = n
			#node.append(n)

		if(test2 >= check*1.0/2**s):
			alert2 = 1
			node = n2
			#node.append(n2)

		if(s == 1 and alert1 == 0):
			alert = 0
			test = test1
			break

 		if(s == 1 and alert2 == 0):
			alert = 0
			test = test2
			break

		if(s > 1 and alert1 == 1):
			alert = 1
			test = test1
			break

 		if(s > 1 and alert2 == 1):
			alert = 1
			test = test2
			break

		# check for no data: delete lines with node = c for latest data

	return alert,i,node,s,test #check for all adjacent alerts as triggers


def setColumnAlert(node_alerts):
	nodes_to_check = NODES_TO_CHECK
	data_file, sensor_nodes, col = getSensorConstants()
	alerts = np.asarray(['sensor','node','cascaded','column_alert','node_alert','xzvel','xyvel','n','test','1/2**n'])
	alerts_raw = alerts
	data_slice = []

	for f in data_file:
		sensor = f + '_proc_chunked.csv'
		max_nodes = sensor_nodes[data_file.index(f)]
		s = node_alerts[node_alerts.sensor == sensor]
		s = s[s.node_alert > 0] #get only node alerts > 0
		ss = []

		if(s.shape[0] == 0):
				continue

		if(s.shape[0] < max_nodes/2):
			temp = [sensor,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN]
			alerts_raw = np.vstack((alerts_raw,np.asarray(temp)))
			continue

		for i in range(max_nodes):
			node_data = s[s.node == i]
			if(node_data.shape[0] == 0):
				temp = [sensor,i,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN]
				alerts_raw = np.vstack((alerts_raw,np.asarray(temp)))
				continue

			if(i < nodes_to_check):
				start_i = 0
			else:
				start_i = i - nodes_to_check

			s_check = max(s.ix[s.node == i,['xyvel','xzvel']].values.tolist()[0])

			#for test
			data_slice = s.loc[s.node >= start_i]
			data_slice = data_slice[data_slice.node <= i + nodes_to_check]

			if(data_slice.shape[0] > 0):
				alert,i,affected,n,test = checkValidColumnAlert(i,data_slice,s_check,nodes_to_check,max_nodes)
				node_data_list = node_data.values[0].tolist()
				temp = [sensor,i,affected,alert,node_data_list[2],node_data_list[4],node_data_list[6],n,test,s_check/2**n]
				alerts_raw = np.vstack((alerts_raw,np.asarray(temp)))
				if(alert > 0):
					alerts = np.vstack((alerts,np.asarray(temp)))
			else:
				continue

	#print alerts.size,alerts_raw.size

	if (alerts.size == 10):
		col_alert = 0
	else:
		col_alert = pd.DataFrame(alerts[1:,:], columns = ['sensor','node','cascaded','column_alert','node_alert','xzvel','xyvel','n','test','1/2**n'])
		col_alert.to_csv(INPUT_FILE_PATH + 'ColumnAlerts.csv', mode = 'a')

	##compare: current node: get all max velocity.. check with alert 1&2.. check max_vel above and below: node_to_be_compared: 1/2n

	if (alerts_raw.size == 10):
		col_alert_raw = 0
	else:
		col_alert_raw = pd.DataFrame(alerts_raw[1:,:], columns = ['sensor','node','cascaded','column_alert','node_alert','xzvel','xyvel','n','test','1/2**n'])
		col_alert_raw.to_csv(INPUT_FILE_PATH + 'ColumnAlertsRaw.csv', mode = 'a')

	return col_alert, col_alert_raw

def getDataRange(date_str):
	print "Data chunks being generated."
	try:
		print "Deleting all previous data..."
		os.remove(INPUT_FILE_PATH + 'node_alerts.csv')
		os.remove(INPUT_FILE_PATH + 'ricsatalerts.csv')
		os.remove(INPUT_FILE_PATH + 'xy_fine.csv')
		os.remove(INPUT_FILE_PATH + 'xy.csv')
		os.remove(INPUT_FILE_PATH + 'xz_fine.csv')
		os.remove(INPUT_FILE_PATH + 'xz.csv')
	except:
		print "Previous data... not found"


	data_file, sensor_nodes, col = getSensorConstants()
	col = ['Time','Node_Number', 'x', 'y', 'z', 'good_data', 'xz', 'xy', 'phi', 'rho', 'moisture', 'moisture_filter']

	#now_date = datetime.now()
	now_date = datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S')
	print now_date

	for f in data_file:
		sensor = f + '_proc.csv'
		dataFrame = pd.DataFrame()

		try:
			os.remove(INPUT_FILE_PATH + f + '_proc_chunked.csv')
		except:
			print "No previous chunk for: " + sensor

		try:
			data = pd.read_csv(INPUT_FILE_PATH + sensor, names = col, parse_dates = ['Time'], chunksize = 99)
		except:
			print "No data for: " + sensor
			continue

		chunk_c = 0
		for chunk in data:
			chunk.set_index(['Time'], drop = False, inplace = True)
			chunk_c = chunk_c + 1
			#print f, chunk_c
			#print chunk.head()
			#print chunk.tail()
			#print chunk.index.min(), chunk.index.max()
			#print 'chunk:', chunk['Time'].min(), chunk['Time'].max(), chunk.shape, 'sensor:', f, 'now:', now_date
			if(chunk.index.max() > now_date - timedelta(3)):
				dataFrame = dataFrame.append(chunk,ignore_index=True)
				#print 'chunk added to dataframe'

		print 'sensor data:', f, chunk['Time'].min(), chunk['Time'].max(), dataFrame.shape, 'chunks', chunk_c

		path = INPUT_FILE_PATH + f + '_proc_chunked.csv'

		try:
			dataFrame.to_csv(path, index_label=None, index=None, cols=col, header=None)
		except:
			print "No data chunk created for: " + sensor
			continue

def updateSensorFiles():
	data_file, sensor_nodes, col = getSensorConstants()
	for data in data_file:
		num_nodes = data_file.index(data)
		ricsat.update_proc_file(data,num_nodes)

def processDynaslope(date_str):
	now_date = datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S')
	setNodeAlert(now_date) 	#to reset files uncomment writetocsv on setNodeAlert()
	node_alerts = loadNodeAlerts()
	node_alerts.to_csv(INPUT_FILE_PATH + 'NodeAlerts.csv', mode = 'a') #set this to append
	col_alerts, col_alerts_raw = setColumnAlert(node_alerts)
	print "showing columns with alerts:"
	print col_alerts
	print "\nshowing all node alert triggers:"
	print col_alerts_raw
	print node_alerts[node_alerts.node_alert > 0]
	#print node_alerts
	return 0

def main():
	try:
		print "Deleting all previous data..."
		os.remove(INPUT_FILE_PATH + 'NodeAlerts.csv')
		os.remove(INPUT_FILE_PATH + 'ColumnAlertsRaw.csv')
		os.remove(INPUT_FILE_PATH + 'ColumnAlerts.csv')
	except:
		print "No data to cleanup."

	#updateSensorFiles()
	dates = ['2013-11-09 0:0:0','2013-11-10 0:0:0','2013-11-11 0:0:0'] #dates to be analyzed
	for date_str in dates:
		getDataRange(date_str)
		now_date = datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S')
		now_date_str = now_date.strftime('%Y-%m-%d.%H-%M-%s')
		save_dir = OUTPUT_FILE_PATH + now_date_str
		os.mkdir(save_dir)
		processDynaslope(date_str)

if __name__ == '__main__':
	startTime = datetime.now()
	print "start processing ..."
	main()
	print 'time:', (datetime.now()-startTime)
	sys.exit()


#TO DOs 18 Dec 2013
# Display: 3 day data window:
# senslopetest.comlu.com
# export to svg plots for column alerts

