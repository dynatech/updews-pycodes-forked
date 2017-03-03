##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ion()

import os
import pandas as pd
import numpy as np
from datetime import date, time, datetime, timedelta
from scipy.stats import spearmanr
from mpl_toolkits.axes_grid.anchored_artists import AnchoredText
import time
import platform

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver


#### Global Parameters
data_path = os.path.dirname(os.path.realpath(__file__))

tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

for i in range(len(tableau20)):    
    r, g, b = tableau20[i]    
    tableau20[i] = (r / 255., g / 255., b / 255.)

global marker_history_edits
marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
index = 0
operation = 'none'
at  = AnchoredText(operation.title(),prop=dict(size=8), frameon=True,loc = 2)

def RemoveDuplicatesAndNone(df):
    UpperSiteCodeUpperMarkerName(df)
    df.drop_duplicates(subset = ['ts','site_code','marker_name','data_source'],keep = 'last',inplace = True)
    df = df[df.operation != 'none']
    return df

def onpress(event):
    global marker_history_edits
    global index
    global operation
    global at
    ax = event.inaxes
    fig = event.canvas
    
    if at.get_visible():
        at.set_visible(not at.get_visible())
        fig.draw()

    if event.key == 'control':
        operation = 'reposition'
        color = tableau20[16]

    elif event.key == 'alt':
        operation = 'mute'
        color = tableau20[6]
    elif event.key == 'q':
        marker_history_edits = RemoveDuplicatesAndNone(marker_history_edits)
        print marker_history_edits
    elif event.key == 'r':
        for axes in ax.figure.get_axes():
            for line in axes.get_lines():
                if np.logical_not(np.logical_or(line.get_label()[:3] == 'SMS',line.get_label()[:3] == 'DRS')):
                    line.remove()
        marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
        ax.figure.canvas.draw()
        print "\nEDITS have been CLEARED"
    elif event.key == 'enter':
        cur_history = pd.read_csv('markerhistory.csv')
        cur_history = RemoveDuplicatesAndNone(cur_history)
        try:
            with open('markerhistory.csv','wb') as mhcsv:
                cur_history = RemoveDuplicatesAndNone(cur_history)
                marker_history_edits = RemoveDuplicatesAndNone(marker_history_edits)
                cur_history = cur_history.append(marker_history_edits)
                print marker_history_edits
                cur_history[['ts','site_code','marker_name','operation','data_source','previous_name']].to_csv(mhcsv, header = True,index = False)
                mhcsv.close()
            marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
            print "^^^ the edits above have been successfuly saved!"
        except:
            print "Error in saving edits, check csv file."
    else:
        operation = 'none'

#        if event.key == 's':
#            out_file_name = '

    if (event.key == 'control' or event.key =='alt') and event.inaxes:
        at  = AnchoredText(operation.title(),prop=dict(size=8), frameon=True,loc = 2)
        at.patch.set_facecolor(color)
        at.patch.set_alpha(0.5)
        ax.add_artist(at)
        ax.figure.canvas.draw()
    
    if not event.inaxes:
        operation = 'none'

def onclick(event):
    global index
    global operation
    line = event.artist
    ax = line.axes
    fig = event.canvas
    xdata, ydata = line.get_data()
    label = line.get_label()
    ind = event.ind
    
    if operation == 'mute':
        print "\nMUTE data point"
    elif operation == 'reposition':
        print "\nREPOSITION data point"
    else:
        print "\nData point"      
    
    if operation == 'mute' or operation == 'reposition':
        print '\ntimestamp: {}\nsite code: {}\nmarker name: {}\ndata source: {}\n\n'.format(pd.to_datetime(xdata[ind][0]).strftime('%m/%d/%Y %H:%M:%S'),label[4:7],label[8:],label[:3])
    marker_history_edits.loc[index,['ts']] = pd.to_datetime(xdata[ind][0])
    marker_history_edits.loc[index,['data_source']] = label[0:3]
    marker_history_edits.loc[index,['site_code']] = label[4:7]
    marker_history_edits.loc[index,['marker_name']] = label[8:]
    marker_history_edits.loc[index,['operation']] = operation
    
    
    if operation == 'mute':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[6])
    elif operation == 'reposition':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[16])
    elif operation == 'none' or not(operation == operation):
        if label[:3] == 'SMS':
            ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[0])
        elif label[:3] == 'DRS':
            ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[4])
#    print ax._facecolors[event.ind,:]
    index += 1
    if operation != 'none':
        visible = at.get_visible()
        at.set_visible(not visible)
        fig.draw()

    
    operation = 'none'
    
    ax.figure.canvas.draw()
    
def onpress_edit(event):
    global marker_history_edits
    global index
    global operation
    global at
    ax = event.inaxes
    fig = event.canvas
    
    if at.get_visible():
        at.set_visible(not at.get_visible())
        fig.draw()

    if event.key == 'control':
        operation = 'reposition'
        color = tableau20[16]

    elif event.key == 'alt':
        operation = 'mute'
        color = tableau20[6]
    elif event.key == 'q':
        marker_history_edits = RemoveDuplicatesAndNone(marker_history_edits)
    elif event.key == 'r':
        for axes in ax.figure.get_axes():
            for line in axes.get_lines():
                if np.logical_not(np.logical_or(line.get_label()[:3] == 'SMS',line.get_label()[:3] == 'DRS')):
                    line.remove()
        marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
        ax.figure.canvas.draw()
        print "\nEDITS have been CLEARED"
    elif event.key == 'enter':
        cur_history = pd.read_csv('markerhistory.csv')
        cur_history = RemoveDuplicatesAndNone(cur_history)
        try:
            with open('markerhistory.csv','wb') as mhcsv:
                marker_history_edits = RemoveDuplicatesAndNone(marker_history_edits)
                cur_history = cur_history.append(marker_history_edits,ignore_index = True)
                cur_history = RemoveDuplicatesAndNone(cur_history)
                cur_history = cur_history[cur_history.operation != 'delete']
                print marker_history_edits
                cur_history[['ts','site_code','marker_name','operation','data_source','previous_name']].to_csv(mhcsv, header = True,index = False)
                mhcsv.close()
            marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
            print "^^^ the edits above have been successfully saved."
        except:
            print "Error in saving edits, check csv file."
    elif event.key == 'delete':
        operation = 'delete'
        color = tableau20[14]
    else:
        operation = 'none'

#        if event.key == 's':
#            out_file_name = '
    if (event.key == 'control' or event.key =='alt' or event.key == 'delete') and event.inaxes:
        at  = AnchoredText(operation.title(),prop=dict(size=8), frameon=True,loc = 2)
        at.patch.set_facecolor(color)
        at.patch.set_alpha(0.5)
        ax.add_artist(at)
        ax.figure.canvas.draw()
    
    if not event.inaxes:
        operation = 'none'

def onclick_edit(event):
    global index
    global operation
    line = event.artist
    ax = line.axes
    fig = event.canvas
    xdata, ydata = line.get_data()
    label = line.get_label()
    ind = event.ind
    
    if operation == 'mute':
        print "\nMUTE data point"
    elif operation == 'reposition':
        print "\nREPOSITION data point"
    elif operation == 'delete':
        print "\nDELETE history for data point"
    else:
        print "\nData point"    
    
    if operation == 'delete' or operation == 'mute' or operation == 'reposition':
        print '\ntimestamp: {}\nsite code: {}\nmarker name: {}\ndata source: {}\n\n'.format(pd.to_datetime(xdata[ind][0]).strftime('%m/%d/%Y %H:%M:%S'),label[4:7],label[8:],label[:3])

    marker_history_edits.loc[index,['ts']] = pd.to_datetime(xdata[ind][0])
    marker_history_edits.loc[index,['data_source']] = label[0:3]
    marker_history_edits.loc[index,['site_code']] = label[4:7]
    marker_history_edits.loc[index,['marker_name']] = label[8:]
    marker_history_edits.loc[index,['operation']] = operation

#    circ = plt.Circle((xdata[ind][0],ydata[ind][0]), radius = line.get_markersize(), color = 'g',zorder = 3)
    if operation == 'mute':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[6],label = 'dummy')
    elif operation == 'reposition':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[16],label = 'dummy')
    elif operation == 'delete':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[14],label = 'dummy')
    elif operation == 'none' or not(operation == operation):
        if label[:3] == 'SMS':
            ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[0],label = 'dummy')
        elif label[:3] == 'DRS':
            ax.plot(xdata[ind][0],ydata[ind][0],'o',color = tableau20[4],label = 'dummy')
#    print ax._facecolors[event.ind,:]
    index += 1
    if operation != 'none':
        visible = at.get_visible()
        at.set_visible(not visible)
        fig.draw()

    
    operation = 'none'
    
    ax.figure.canvas.draw()

def onpress_cumdisp(event):
    global marker_history_edits
    global index
    global operation
    global at
    global color
    
    ax = event.inaxes
    if ax:
        cur_color = ax.get_lines()[0].get_color()
        color = cur_color
    else:
        cur_color = tableau20[0]
        
    fig = event.canvas
    
    if at.get_visible():
        at.set_visible(not at.get_visible())
        fig.draw()

    if event.key == 'control':
        operation = 'reposition'
        color = tableau20[(tableau20.index(cur_color) + 10)%20]
    elif event.key == 'alt':
        operation = 'mute'
        color = tableau20[(tableau20.index(cur_color) + 14)%20]
    elif event.key == 'q':
        marker_history_edits = RemoveDuplicatesAndNone(marker_history_edits)
        print marker_history_edits
    elif event.key == 'r':
        for axes in ax.figure.get_axes():
            for line in axes.get_lines():
                if line.get_label()[:5] == 'dummy':
                    line.remove()
        marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
        ax.figure.canvas.draw()
        print "\nEDITS have been CLEARED"
    elif event.key == 'enter':
        cur_history = pd.read_csv('markerhistory.csv')
        cur_history = RemoveDuplicatesAndNone(cur_history)

        try:
            with open('markerhistory.csv','wb') as mhcsv:
                cur_history = RemoveDuplicatesAndNone(cur_history)
                marker_history_edits = RemoveDuplicatesAndNone(marker_history_edits)
                cur_history = cur_history.append(marker_history_edits)
                print marker_history_edits
                cur_history[['ts','site_code','marker_name','operation','data_source','previous_name']].to_csv(mhcsv, header = True,index = False)
                mhcsv.close()
            marker_history_edits = pd.DataFrame(columns = ['ts','site_code','marker_name','operation','data_source'])
            print "^^^the edits above have been successfully saved!"
        except:
            print "Error in saving, check csv file."
    else:
        operation = 'none'

    if (event.key == 'control' or event.key =='alt') and event.inaxes:
        at  = AnchoredText(operation.title(),prop=dict(size=8), frameon=True,loc = 2)
        at.patch.set_facecolor(color)
        at.patch.set_alpha(0.5)
        ax.add_artist(at)
        ax.figure.canvas.draw()
    
    if not event.inaxes:
        operation = 'none'
        
        

def onclick_cumdisp(event):
    global index
    global operation
    line = event.artist
    ax = line.axes
    fig = event.canvas
    xdata, ydata = line.get_data()
    label = line.get_label()
    ind = event.ind
    if operation == 'mute':
        print "\nMUTE data point"
    elif operation == 'reposition':
        print "\nREPOSITION data point"
    else:
        print "\nData point"
    if operation == 'mute' or operation == 'reposition':
        print '\ntimestamp: {}\nsite code: {}\nmarker name: {}\ndata source: {}\n\n'.format(pd.to_datetime(xdata[ind][0]).strftime('%m/%d/%Y %H:%M:%S'),label[4:7],label[8:],label[:3])

    marker_history_edits.loc[index,['ts']] = pd.to_datetime(xdata[ind][0])
    marker_history_edits.loc[index,['data_source']] = label[0:3]
    marker_history_edits.loc[index,['site_code']] = label[4:7]
    marker_history_edits.loc[index,['marker_name']] = label[8:]
    marker_history_edits.loc[index,['operation']] = operation
    
    
    if operation == 'mute':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = color,label = 'dummy')
    elif operation == 'reposition':
        ax.plot(xdata[ind][0],ydata[ind][0],'o',color = color,label = 'dummy')
    elif operation == 'none' or not(operation == operation):
        cur_color = line.get_color()
        if label[:3] == 'SMS':
            ax.plot(xdata[ind][0],ydata[ind][0],'o',color = cur_color,label = 'dummy')
        elif label[:3] == 'DRS':
            ax.plot(xdata[ind][0],ydata[ind][0],'o',color = cur_color,label = 'dummy')
#    print ax._facecolors[event.ind,:]
    index += 1
    if operation != 'none':
        visible = at.get_visible()
        at.set_visible(not visible)
        fig.draw()

    
    operation = 'none'
    
    ax.figure.canvas.draw()


def UpperSiteCodeUpperMarkerName(df):
    df['site_code'] = map(lambda x: x.upper(),df['site_code'])
    df['marker_name'] = map(lambda x: x.title(),df['marker_name'])
    df['ts'] = map(lambda x: pd.to_datetime(x),df['ts'])
    try:
        df['previous_name'] = map(lambda x: x.title(),df['previous_name'])
    except:
        pass

def RenameMarkers(surficial_data,rename_history):
    for site_code, new_name, prev_name in rename_history[['site_code','marker_name','previous_name']].values:
        mask = np.logical_and(surficial_data.site_code == site_code,surficial_data.marker_name == prev_name)
        surficial_data.loc[mask,['marker_name']] = new_name

def MuteMarkers(surficial_data,mute_history):
    df = pd.merge(surficial_data,mute_history,how = 'left',on = ['ts','site_code','marker_name','data_source'])
    df = df[df.operation.isnull()]
    return df[['site_code','marker_name','ts','meas_type','observer_name','weather','data_source','meas','reliability']]

def ComputeDisplacementMarker(marker_df,reposition_history):
    marker_df.sort_values(['ts'],inplace = True)
    marker_df['displacement'] = (marker_df['meas'] - marker_df['meas'].shift()).fillna(0)
    
    for ts, site_code, marker_name,data_source in reposition_history[['ts', 'site_code', 'marker_name','data_source']].values:
        mask = np.logical_and(marker_df.site_code == site_code,marker_df.marker_name == marker_name)
        mask = np.logical_and(mask,marker_df.ts == ts)
        mask = np.logical_and(mask,marker_df.data_source == data_source)
        marker_df.loc[mask,['displacement']] = 0

    marker_df['cumulative_displacement'] = marker_df['displacement'].cumsum()
    return marker_df
    
    
def SurficialDataPlot(surficial_csv_file,history_csv_file,mute = True):
    #### Rename and read csv files
    surficial_data = pd.read_csv(data_path + '/'+surficial_csv_file +'.csv')
    history_data = pd.read_csv(data_path + '/'+history_csv_file +'.csv')

    #### Upper caps site_code, title form marker_name
    UpperSiteCodeUpperMarkerName(surficial_data)
    UpperSiteCodeUpperMarkerName(history_data)
    
    #### Rename markers
    rename_history = history_data[history_data.operation == 'rename']
    RenameMarkers(surficial_data,rename_history)
    
    #### Mute markers if mute = True
    if mute:    
        mute_history = history_data[history_data.operation == 'mute']
        surficial_data = MuteMarkers(surficial_data,mute_history)
    
    
    
    #### Determine sites and markers to plot
    sites_to_plot = np.unique(surficial_data.site_code.values)
    markers_to_plot = []
    
    print "Plotting {} site/s: {}\n".format(len(sites_to_plot),', '.join(sites_to_plot))    
    
    for site in sites_to_plot:
        markers = np.unique(surficial_data.loc[surficial_data.site_code == site,['marker_name']].values)
        print "Plotting {} marker/s for site {}: {}".format(len(markers),site,', '.join(markers))
        markers_to_plot.append(markers)
    
    #### Set the number of plots per page

    fig_num = 0            
    for site, markers in zip(sites_to_plot,markers_to_plot):
        plots_per_page = 3
        #### Add fig every site, reset plot num to 1, reset axes      
        fig_num += 1
        plot_num = 1
        all_plot_num = 0
        cur_ax = None
        
        for marker in markers:
            #### Set to minimum if number of markers to plot is less than plots_per_page
            if len(markers) < plots_per_page:
                plots_per_page = len(markers)
            
            #### Set correct figure and axes
            cur_fig = plt.figure(fig_num)
            cur_ax = cur_fig.add_subplot(plots_per_page,1,plot_num,sharex = cur_ax)
            cur_ax.grid()
            cur_ax.tick_params(labelbottom = 'off')
            
            #### Set x and y labels
            cur_fig.text(0.05,0.5,'Measurement, cm',va='center',rotation = 'vertical',fontsize = 16)
            cur_ax.set_ylabel('{}'.format(marker),fontsize = 15)
            cur_fig.text(0.5,0.04,'Timestamp',ha = 'center',fontsize = 17)
            #### Obtain data to plot
            data_mask = np.logical_and(surficial_data.site_code == site,surficial_data.marker_name == marker)
            drs_mask = np.logical_and(data_mask,surficial_data.data_source == 'DRS')
            sms_mask = np.logical_and(data_mask,surficial_data.data_source == 'SMS')
            
            drs_ts_data = surficial_data.loc[drs_mask,['ts']].values            
            drs_meas_data = surficial_data.loc[drs_mask,['meas']].values
            sms_ts_data = surficial_data.loc[sms_mask,['ts']].values            
            sms_meas_data = surficial_data.loc[sms_mask,['meas']].values
            
            #### Plot values to current axis
            
            cur_sms_plot = cur_ax.plot(sms_ts_data,sms_meas_data,'o-',color = tableau20[0],label = 'SMS {} {}'.format(site,marker))
            cur_drs_plot = cur_ax.plot(drs_ts_data,drs_meas_data,'o-',color = tableau20[4],label = 'DRS {} {}'.format(site,marker))
            
            #### Plot picker points
            cur_ax.plot(sms_ts_data,sms_meas_data,'o',color = tableau20[0],label = 'SMS {} {}'.format(site,marker),picker = 5)
            cur_ax.plot(drs_ts_data,drs_meas_data,'o',color = tableau20[4],label = 'DRS {} {}'.format(site,marker),picker = 5)

            
            #### Set parameters for next plot page
            plot_num += 1
            all_plot_num += 1
            if plot_num > plots_per_page or marker == markers[-1]:
                plots = cur_sms_plot + cur_drs_plot            
                labels = [l.get_label()[:3] for l in plots]
                
                #### Show x-axis ticker at the last plot axes. Set the date format.
                cur_ax.xaxis.set_major_formatter(md.DateFormatter('%d %b %Y'))
                cur_ax.tick_params(labelbottom = 'on')
                cur_fig.autofmt_xdate()
                cur_fig.legend(plots,labels,'center right',fontsize = 15)                
                cur_fig.suptitle('Surficial Markers Measurement for Site {}'.format(site),fontsize = 17)
                cur_fig.canvas.mpl_connect('pick_event',onclick)
                cur_fig.canvas.mpl_connect('key_press_event',onpress)
                
                #### Set plots per page to a minimum if number of markers to plot at the next page is less than plots per page
                if len(markers) - all_plot_num < plots_per_page:
                    plots_per_page = len(markers) - all_plot_num
                
                if marker != markers[-1]:
                    fig_num += 1
                plot_num = plot_num - plots_per_page               
            
        
def CumulativeDisplacementPlot(surficial_csv_file,history_csv_file,reposition = True,mute = True):
    #### Rename and read csv files
    surficial_data = pd.read_csv(data_path + '/'+surficial_csv_file +'.csv')
    history_data = pd.read_csv(data_path + '/'+history_csv_file +'.csv')
    
    #### Upper caps site_code, title form marker_name
    UpperSiteCodeUpperMarkerName(surficial_data)
    UpperSiteCodeUpperMarkerName(history_data)
    
    #### Rename markers
    rename_history = history_data[history_data.operation == 'rename']
    RenameMarkers(surficial_data,rename_history)

    #### Mute markers if mute = True
    if mute:    
        mute_history = history_data[history_data.operation == 'mute']
        surficial_data = MuteMarkers(surficial_data,mute_history)        
    
    #Get dataframe columns
    columns = surficial_data.columns.values    
    
    #### Compute for marker displacement consider repositioned markers if reposition = True
    if reposition:    
        reposition_history = history_data[history_data.operation == 'reposition']
    else:
        reposition_history = pd.DataFrame(columns=columns)
    
    surficial_data_group = surficial_data.groupby(['marker_name'],as_index = False)
    surficial_data = surficial_data_group.apply(ComputeDisplacementMarker,reposition_history).reset_index()
    
    #### Add displacement and cumulative_displacement columns
    columns = np.append(columns,['displacement','cumulative_displacement'])
    surficial_data = surficial_data[columns]
    
    #### Determine sites and markers to plot
    sites_to_plot = np.unique(surficial_data.site_code.values)
    markers_to_plot = []
    
    print "Plotting {} site/s: {}\n".format(len(sites_to_plot),', '.join(sites_to_plot))    
    
    for site in sites_to_plot:
        markers = np.unique(surficial_data.loc[surficial_data.site_code == site,['marker_name']].values)
        print "Plotting {} marker/s for site {}: {}".format(len(markers),site,', '.join(markers))
        markers_to_plot.append(markers)
    
    #### Set the number of plots per page

    fig_num = 0  
    for site, markers in zip(sites_to_plot,markers_to_plot):
        plots_per_page = 3
        #### Add fig every site, reset plot num to 1, reset axes      
        fig_num += 1
        plot_num = 1
        all_plot_num = 0
        cur_ax = None
        
        for marker in markers:

            #### Set to minimum if number of markers to plot is less than plots_per_page
            if len(markers) < plots_per_page:
                plots_per_page = len(markers)
            
            #### Set correct figure and axes
            cur_fig = plt.figure(fig_num)
            cur_ax = cur_fig.add_subplot(plots_per_page,1,plot_num,sharex = cur_ax)
            cur_ax.grid()
            cur_ax.tick_params(labelbottom = 'off')
            
            #### Set x and y labels
            cur_fig.text(0.05,0.5,'Cumulative Displacement, cm',va='center',rotation = 'vertical',fontsize = 16)
            cur_fig.text(0.5,0.04,'Timestamp',ha = 'center',fontsize = 17)
            
            #### Obtain data to plot
            data_mask = np.logical_and(surficial_data.site_code == site,surficial_data.marker_name == marker)            
            cur_ts_data = surficial_data[data_mask]['ts'].values            
            cur_cumdisp_data = surficial_data[data_mask]['cumulative_displacement'].values
            
            sms_mask = np.logical_and(data_mask,surficial_data.data_source == 'SMS')
            sms_ts = surficial_data[sms_mask]['ts'].values
            sms_cumdisp = surficial_data[sms_mask]['cumulative_displacement'].values
            
            drs_mask = np.logical_and(data_mask,surficial_data.data_source == 'DRS')
            drs_ts = surficial_data[drs_mask]['ts'].values
            drs_cumdisp = surficial_data[drs_mask]['cumulative_displacement'].values
            
            #### Plot values to current axis
            cur_ax.plot(cur_ts_data,cur_cumdisp_data,'o-',color = tableau20[all_plot_num*2],label = '{}'.format(marker))

            #### Plot picker points
            cur_ax.plot(sms_ts,sms_cumdisp,'o',color = tableau20[all_plot_num*2],label = 'SMS {} {}'.format(site,marker),picker = 5)
            cur_ax.plot(drs_ts,drs_cumdisp,'o',color = tableau20[all_plot_num*2],label = 'DRS {} {}'.format(site,marker),picker = 5)

            #### Set parameters for next plot page
            plot_num += 1
            all_plot_num += 1
            if plot_num > plots_per_page or marker == markers[-1]:
                
                #### Show x-axis ticker at the last plot axes. Set the date format.
                cur_ax.tick_params(labelbottom = 'on')
                cur_ax.xaxis.set_major_formatter(md.DateFormatter('%d %b %Y'))
                cur_fig.autofmt_xdate()
                #### Get legend for all plots
                lines = []
                for ax in cur_ax.figure.get_axes():
                    for line in ax.get_lines():
                        if np.logical_not(np.logical_or(line.get_label()[:3] == 'SMS',line.get_label()[:3] == 'DRS')):
                            lines.append(line)
                labels = [l.get_label() for l in lines]
                cur_fig.legend(lines,labels,'center right',fontsize = 15)                
                cur_fig.suptitle('Cumulative Displacement Plot for Site {}'.format(site),fontsize = 17)
                
                #### Activate Picker
                cur_fig.canvas.mpl_connect('pick_event',onclick_cumdisp)
                cur_fig.canvas.mpl_connect('key_press_event',onpress_cumdisp)             
                
                #### Set plots per page to a minimum if number of markers to plot at the next page is less than plots per page
                if len(markers) - all_plot_num < plots_per_page:
                    plots_per_page = len(markers) - all_plot_num
                
                if marker != markers[-1]:
                    fig_num += 1
                plot_num = plot_num - plots_per_page               

def ViewHistory(surficial_csv_file,history_csv_file):
    #### Rename and read csv files
    surficial_data = pd.read_csv(data_path + '/'+surficial_csv_file +'.csv')
    history_data = pd.read_csv(data_path + '/'+history_csv_file +'.csv')

    #### Upper caps site_code, title form marker_name
    UpperSiteCodeUpperMarkerName(surficial_data)
    UpperSiteCodeUpperMarkerName(history_data)
    
    #### Rename markers
    rename_history = history_data[history_data.operation == 'rename']
    RenameMarkers(surficial_data,rename_history)

    #### Determine sites and markers to plot
    sites_to_plot = np.unique(surficial_data.site_code.values)
    markers_to_plot = []
    
    print "Plotting {} site/s: {}\n".format(len(sites_to_plot),', '.join(sites_to_plot))    
    
    for site in sites_to_plot:
        markers = np.unique(surficial_data.loc[surficial_data.site_code == site,['marker_name']].values)
        print "Plotting {} marker/s for site {}: {}".format(len(markers),site,', '.join(markers))
        markers_to_plot.append(markers)
    
    #### Set the number of plots per page

    fig_num = 0            
    for site, markers in zip(sites_to_plot,markers_to_plot):
        plots_per_page = 3
        #### Add fig every site, reset plot num to 1, reset axes      
        fig_num += 1
        plot_num = 1
        all_plot_num = 0
        cur_ax = None
        
        for marker in markers:
            #### Set to minimum if number of markers to plot is less than plots_per_page
            if len(markers) < plots_per_page:
                plots_per_page = len(markers)
            
            #### Set correct figure and axes
            cur_fig = plt.figure(fig_num)
            cur_ax = cur_fig.add_subplot(plots_per_page,1,plot_num,sharex = cur_ax)
            cur_ax.grid()
            cur_ax.tick_params(labelbottom = 'off')
            
            #### Set x and y labels
            cur_fig.text(0.05,0.5,'Measurement, cm',va='center',rotation = 'vertical',fontsize = 16)
            cur_ax.set_ylabel('{}'.format(marker),fontsize = 15)
            cur_fig.text(0.5,0.04,'Timestamp',ha = 'center',fontsize = 17)
            
            #### Obtain data to plot
            data_mask = np.logical_and(surficial_data.site_code == site,surficial_data.marker_name == marker)
            
            drs_mask = np.logical_and(data_mask,surficial_data.data_source == 'DRS')
            sms_mask = np.logical_and(data_mask,surficial_data.data_source == 'SMS')
            
            drs_ts_data = surficial_data.loc[drs_mask,['ts']].values            
            drs_meas_data = surficial_data.loc[drs_mask,['meas']].values
            sms_ts_data = surficial_data.loc[sms_mask,['ts']].values            
            sms_meas_data = surficial_data.loc[sms_mask,['meas']].values
            
            history_mask = np.logical_and(history_data.site_code == site,history_data.marker_name == marker)         
            mute_mask = np.logical_and(history_mask,history_data.operation == 'mute')
            reposition_mask = np.logical_and(history_mask,history_data.operation == 'reposition')
            
            mute_history_data = pd.merge(history_data[mute_mask],surficial_data,how = 'left',on = ['ts','site_code','marker_name','data_source'])            
            reposition_history_data = pd.merge(history_data[reposition_mask],surficial_data,how = 'left',on = ['ts','site_code','marker_name','data_source'])            
            
            mute_ts = mute_history_data['ts'].values
            mute_meas = mute_history_data['meas'].values
            
            reposition_ts = reposition_history_data['ts'].values
            reposition_meas = reposition_history_data['meas'].values
            
            #### Plot values to current axis
            
            cur_sms_plot = cur_ax.plot(sms_ts_data,sms_meas_data,'o-',color = tableau20[0],label = 'SMS {} {}'.format(site,marker))
            cur_drs_plot = cur_ax.plot(drs_ts_data,drs_meas_data,'o-',color = tableau20[4],label = 'DRS {} {}'.format(site,marker))
            
            
            cur_ax.plot(sms_ts_data,sms_meas_data,'o',color = tableau20[0],label = 'SMS {} {}'.format(site,marker),picker = 5)
            cur_ax.plot(drs_ts_data,drs_meas_data,'o',color = tableau20[4],label = 'DRS {} {}'.format(site,marker),picker = 5)
            
            #### Plot history values
            cur_mute_plot = cur_ax.plot(mute_ts,mute_meas,'o',color = tableau20[6],label = 'Mute')
            cur_reposition_plot = cur_ax.plot(reposition_ts,reposition_meas,'o',color = tableau20[16],label = 'Reposition')
            
            #### Set parameters for next plot page
            plot_num += 1
            all_plot_num += 1
            if plot_num > plots_per_page or marker == markers[-1]:
                plots = cur_sms_plot + cur_drs_plot + cur_mute_plot + cur_reposition_plot         
                labels = [l.get_label()[:4] for l in plots]
                
                #### Show x-axis ticker at the last plot axes. Set the date format.
                cur_ax.xaxis.set_major_formatter(md.DateFormatter('%d %b %Y'))
                cur_ax.tick_params(labelbottom = 'on')
                cur_fig.autofmt_xdate()
                cur_fig.legend(plots,labels,'center right',fontsize = 15)                
                cur_fig.suptitle('Surficial Markers Measurement for Site {}'.format(site),fontsize = 17)
                cur_fig.canvas.mpl_connect('pick_event',onclick_edit)
                cur_fig.canvas.mpl_connect('key_press_event',onpress_edit)
                
                #### Set plots per page to a minimum if number of markers to plot at the next page is less than plots per page
                if len(markers) - all_plot_num < plots_per_page:
                    plots_per_page = len(markers) - all_plot_num
                
                if marker != markers[-1]:
                    fig_num += 1
                plot_num = plot_num - plots_per_page               


######################################
#############    MAIN    #############
######################################
print "\n\n########################################################################"
print "##      Surficial Marker Measurements Plotter and History Writer      ##"
print "########################################################################\n"

while True:
    sur_input = raw_input("Input surficial data csv filename: ")
    
    if sur_input[-4:] == '.csv':
        surficial_csv_file = sur_input[:-4]
    elif len(sur_input) == 0:
        pass
    else:
        surficial_csv_file = sur_input


    try:
        df = pd.read_csv(data_path + '/'+surficial_csv_file +'.csv')
        break
    except:
        print "Error in the filename/directory."

while True:
    his_input = raw_input("Input marker history data csv filename: ")
    
    if his_input[-4:] == '.csv':
        history_csv_file = his_input[:-4]
    elif len(his_input) == 0:
        pass
    else:
        history_csv_file = his_input

    try:
        df = pd.read_csv(data_path + '/'+history_csv_file +'.csv')
        break
    except:
        print "Error in the filename/directory."

while True:
    
    print "\n\n#################################################"
    print "#####   Choose among the following modes:   #####"
    print "#################################################"
    print "\n\nSMP (Surficial Measurements Plot) - marker measurements plotted versus timestamp, SMS & DRS data are discriminated.\n"
    print "CDP (Cumulative Displacement Plot) - cumulative displacement plotted versus timestamp."
    print "\nMHP (Marker History Plot) - marker measurements plotted versus timestamp, SMS & DRS as well as historical data points are discriminated.\n\n"
    
    mode = raw_input("(SMP, CDP, MHP):")
    mode = mode.upper()
    
    if mode in ['SMP','CDP','MHP']:
        break
    else:
        print "Choose from the following options (SMP, CDP, MHP):"
        continue

if mode == 'SMP' or mode == 'CDP':
    mute = raw_input("Hide muted points (Y/N)? (default is Y):")
    if mute.upper() == 'Y':
        mute = True
    elif mute.upper() == 'N':
        mute = False
    else:
        mute = True

    if mode == 'CDP':
        reposition = raw_input("Set displacement to zero for repositioned points (Y/N)? (default is Y):")
        if reposition.upper() == 'Y':
            reposition = True
        elif reposition.upper() == 'N':
            reposition = False
        else:
            reposition = True

print "\n\nEntering {} mode".format(mode)
for i in range(10):
    print "."
    time.sleep(0.1)

if mode == 'SMP':
    SurficialDataPlot(surficial_csv_file,history_csv_file,mute)
elif mode == 'CDP':
    CumulativeDisplacementPlot(surficial_csv_file,history_csv_file,mute,reposition)
elif mode == 'MHP':
    ViewHistory(surficial_csv_file,history_csv_file)

print "\n\n----------------------------------------------------------"
print "General commands while in interactive mode:\n"
print "Alt + Click: Propose to MUTE the datapoint"
print "Ctrl + Click: Propose to REPOSITION the datapoint"
if mode == 'MHP':
    print "Delete + Click: Propose to DELETE history of the data point"
print "Click: UNDO any edit to the datapoint"
print "R: Refresh all proposed history"
print "Q: View all pending edits"
print "C: Reset view"
print "S: Save figure (current view)"
print "Enter: Save & write edits to history csv file"
print "----------------------------------------------------------"
    
    