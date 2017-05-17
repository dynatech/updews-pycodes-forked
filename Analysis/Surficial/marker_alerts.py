# -*- coding: utf-8 -*-
"""
Created on Tue May 02 15:18:52 2017

@author: LUL
"""


#### Import essential libraries
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerLine2D
import os
import pandas as pd
import numpy as np
import sys
from datetime import datetime, time, timedelta
from scipy.interpolate import UnivariateSpline
from scipy.signal import gaussian
from scipy.ndimage import filters
from sqlalchemy import create_engine


#### Add 'Analysis' folder to python scripts search
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path 

#### Import local codes
import querySenslopeDb as q
import configfileio as cfg
import surficialconfig as scfg
import platform
import querydb as qdb
#### Determine current os
curOS = platform.system()

#### Import corresponding mysql library
if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

#### Open config files
config = cfg.config()
surficialconfig = scfg.config()

#### Create directory
output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

def GaussianWeightedAverage(series,sigma = 3,width = 39):
    """
    Computes for rolling weighted average and variance using a gaussian signal filter
    
    Parameters
    ---------------
    series: Array
        Series to be averaged
    sigma: Float
        Standard deviation of the gaussian filter
    Width: int
        Number of points to create the gaussian filter
    
    Returns
    ---------------
    average: Array
        Rolling weighted average of the series    
    var: Array
        Rolling variance of the series
    """
    
    #### Create the Gaussian Filter
    b = gaussian(width,sigma)
    
    #### Take the rolling average using convolution
    average = filters.convolve1d(series,b/b.sum())
    
    #### Take the variance using convolution
    var = filters.convolve1d(np.power(series-average,2),b/b.sum())
    
    return average,var

def RoundTime(date_time):
    """
    Rounds given date_time to the next alert release time
    """
    date_time = pd.to_datetime(date_time)
    time_hour = int(date_time.strftime('%H'))
    time_float = float(date_time.strftime('%H')) + float(date_time.strftime('%M'))/60

    quotient = time_hour / 4
    if quotient == 5:
        if time_float % 4 > 3.5:
            date_time = datetime.combine(date_time.date() + timedelta(1), time(4,0,0))
        else:
            date_time = datetime.combine(date_time.date() + timedelta(1), time(0,0,0))
    elif time_float % 4 > 3.5:
        date_time = datetime.combine(date_time.date(), time((quotient + 2)*4,0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient + 1)*4,0,0))
            
    return date_time
    
def CreateMarkerAlertsTable():
    """Creates the marker alerts table"""
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()

    cur.execute("USE {}".format(config.dbio.namedb))
    
    query = "CREATE TABLE IF NOT EXISTS marker_alerts(ma_id int AUTO_INCREMENT, ts timestamp, marker_id smallint(6) unsigned, displacement float, time_delta float, alert_level tinyint, PRIMARY KEY (ma_id), FOREIGN KEY (marker_id) REFERENCES markers(marker_id))"
    
    cur.execute(query)
    db.close()

def Tableau20Colors():
    """
    Generates normalized RGB values of tableau20
    """
    tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

    for i in range(len(tableau20)):    
        r, g, b = tableau20[i]    
        tableau20[i] = (r / 255., g / 255., b / 255.)
    return tableau20


def GetSurficialData(site_id,ts,num_pts):
    """
    Retrieves the latest surficial data from marker_id and marker_observations table
    
    Parameters
    --------------
    site_id: int
        site_id of site of interest
    ts: timestamp
        latest datetime of data of interest
    num_pts: int
        number of observations you wish to obtain
        
    Returns
    ------------
    Dataframe
        Dataframe containing surficial data with columns [ts, marker_id, measurement]
    """
    query = "SELECT ts, md1.marker_id, md1.measurement, COUNT(*) num FROM marker_data md1 JOIN marker_data md2 ON md1.mo_id <= md2.mo_id AND md1.marker_id = md2.marker_id AND md1.marker_id in (SELECT marker_id FROM marker_data INNER JOIN marker_observations ON marker_data.mo_id = marker_observations.mo_id AND ts = (SELECT max(ts) FROM marker_observations WHERE site_id = {} AND ts <= '{}')) INNER JOIN marker_observations ON marker_observations.mo_id = md1.mo_id AND ts <= '{}' GROUP BY md1.marker_id, md1.mo_id HAVING COUNT(*) <= {} ORDER by ts desc".format(site_id,ts,ts,num_pts)
    return q.GetDBDataFrame(query)

def GetSurficialDataWindow(site_id,ts_start,ts_end):
    """
    Retrieves the measurement of surficial ground movement for the specified site within the given time range
    
    Parameters
    --------------
    site_id: int
        site_id of site of interest
    ts_start: timestamp
        beginning timestamp of the surficial data window
    ts_end: timestamp
        end timestamp of the surficial data window
        
    Returns
    ------------
    Dataframe
        Dataframe containing surficial data with columns [ts, marker_id, measurement]
    """
    query = "SELECT ts, marker_id, measurement FROM marker_data INNER JOIN marker_observations ON marker_observations.mo_id = marker_data.mo_id AND ts <= '{}' AND ts >= '{}' AND marker_id in (SELECT marker_id FROM marker_data INNER JOIN marker_observations ON marker_data.mo_id = marker_observations.mo_id WHERE ts = (SELECT max(ts) FROM marker_observations WHERE site_id = {} AND ts <= '{}')) ORDER by ts DESC".format(ts_end,ts_start,site_id,ts_end)
    return q.GetDBDataFrame(query)

def GetMarkerDetails(marker_id):
    """
    Gives the site_code and marker name for a given marker id
    
    Parameters
    ---------------
    marker_id: int
        marker_id of interest
    
    Returns
    ----------------
    site_code, marker_name: string, string
    """
    #### Connect to db
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(config.dbio.namedb))
    
    #### select site_code and marker name from tables markers, marker_history, marker_names, and sites
    query = "SELECT site_code,marker_name FROM marker_history INNER JOIN markers ON markers.marker_id = marker_history.marker_id INNER JOIN sites ON markers.site_id = sites.site_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id WHERE marker_history.history_id = (SELECT max(history_id) FROM marker_history WHERE event in ('add','rename') AND marker_id = {})".format(marker_id)
    
    #### Execute query
    cur.execute(query)
    
    #### Fetch result
    site_code, marker_name = cur.fetchone()
    
    #### Close db
    db.close()
    
    return site_code,marker_name

def GetSiteCode(site_id):
    """
    Gives the site code of the given site_id
    
    Parameters
    ----------------
    site_id: int
        site_id of interest
    
    Returns
    ----------------
    site_code: string
        Three letter site code of the corresponding site_id
    """
    #### Connect to db
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(config.dbio.namedb))
    
    #### select site_code and marker name from tables markers, marker_history, marker_names, and sites
    query = "SELECT site_code FROM sites WHERE site_id = {}".format(site_id)
    
    #### Execute query
    cur.execute(query)
    
    #### Fetch result
    site_code = cur.fetchone()[0]
    
    #### Close db
    db.close()
    
    return site_code

def GetMarkerName(marker_id):
    """
    Give the corresponding marker name of the given marker id

    Parameters
    ---------------
    marker_id: int
        Marker id whose marker name is to be determined
    
    Returns
    ---------------
    marker_name: string
        Marker name of the given marker id
    """
    
    #### Connect to db
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(config.dbio.namedb))
    
    #### select site_code and marker name from tables markers, marker_history, marker_names, and sites
    query = "SELECT marker_name FROM marker_history INNER JOIN markers ON markers.marker_id = marker_history.marker_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id WHERE marker_history.history_id = (SELECT max(history_id) FROM marker_history WHERE event in ('add','rename') AND marker_id = {})".format(marker_id)
    
    #### Execute query
    cur.execute(query)
    
    #### Fetch result
    marker_name = cur.fetchone()[0]
    
    #### Close db
    db.close()
    
    return marker_name

def ComputeConfidenceIntervalWidth(velocity):
    """
    Computes for the width of the confidence interval for a given velocity
    
    Parameters
    -------------------
    velocity: array-like
        velocity of interest
    
    Returns
    -------------------
    ci_width: array-like same size as input
        confidence interval width for the corresponding velocities
    """
    
    ### Using tcrit table and Federico 2012 values
    return surficialconfig.values.t_crit*np.sqrt(1/(surficialconfig.values.n-2)*surficialconfig.values.sum_res_square*(1/surficialconfig.values.n + (np.log(velocity) - surficialconfig.values.v_log_mean)**2/surficialconfig.values.var_v_log))

def ComputeCriticalAcceleration(velocity):
    """
    Computes for the critical acceleration and its lower and upper bounds for a given velocity range
    
    Parameters
    ---------------
    velocity: array-like
        velocity of interest
        
    Returns
    ---------------
    crit_acceleration: array-like same size as input
        corresponding critical acceleration for each given velocity
    acceleration_upper_bound: array-like
        upper bound for acceleration
    acceleration_lower_bound: array-like
        lower bound for acceleration
    """
    
    #### Compute for critical acceleration from computed slope and intercept from critical values
    crit_acceleration = np.exp(surficialconfig.values.slope*np.log(velocity) + surficialconfig.values.intercept)
    
    #### Compute for confidence interval width width
    ci_width = ComputeConfidenceIntervalWidth(velocity)
    
    #### Compute for lower and upper bound of acceleration
    acceleration_upper_bound = crit_acceleration*np.exp(ci_width)
    acceleration_lower_bound = crit_acceleration*np.exp(-ci_width)
    
    return crit_acceleration,acceleration_upper_bound,acceleration_lower_bound

def ResetAutoIncrement():
    """
    Reset autoincrement to maximum value after delete
    """
    #### Connect to db
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(config.dbio.namedb))

    ## Get the current maximum
    cur.execute("SELECT max(ma_id) FROM marker_alerts")
    max_id = cur.fetchone()[0]
    
    ## Set new id to current max plus one
    if max_id:
        new_id = max_id + 1
    else:
        new_id = 1
    
    ## Change the current autoincrement to max id value
    cur.execute("ALTER TABLE marker_alerts AUTO_INCREMENT = {}".format(new_id))
    
    ## Commit changes
    db.commit()
    
    db.close()
    
def DeleteDuplicatesMarkerAlertsDB(marker_alerts_df):
    """
    Deletes entries on the database with the same timestamp and marker id as the supplied marker alerts df
    
    Parameters
    ------------------
    marker_alerts_df: Pandas DataFrame
        marker alerts dataframe with columns: [ts, marker_id, displacement, time_delta, alert_level]
    
    Returns
    ------------------
    None
    """
    #### Collect the values to be deleted
    values_to_delete = zip(map(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M'),marker_alerts_df.ts.values),marker_alerts_df.marker_id.values)

    #### Create query
    query = "DELETE FROM marker_alerts WHERE ts = %s AND marker_id = %s "

    #### Connect to db
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(config.dbio.namedb))
    
    #### Use executemany to delete specified values
    cur.executemany(query,values_to_delete)
    
    #### Commit changes 
    db.commit()

    #### Close db
    db.close()

def WriteToMarkerAlertsDB(marker_alerts_df):
    """
    Writes the input marker alerts to the database, replacing any duplicate entries.
    
    Parameters
    --------------------
    marker_alerts_df: Pandas DataFrame
        marker alerts dataframe with columns: [ts, marker_id, displacement, time_delta, alert_level]
    
    Returns
    --------------------
    None
    """
    #### Delete possible duplicates
    DeleteDuplicatesMarkerAlertsDB(marker_alerts_df)
    
    ### Reset the auto increment
    ResetAutoIncrement()
    
    #### Create engine to connect to db
    engine=create_engine('mysql://'+config.dbio.userdb+':'+config.dbio.passdb+'@'+config.dbio.hostdb+':3306/'+config.dbio.namedb)
    
    #### Insert dataframe to the database
    marker_alerts_df.set_index('ts').to_sql(name = 'marker_alerts', con = engine, if_exists = 'append', schema = config.dbio.namedb, index = True)

def PlotMarkerMeas(marker_data_df,colors):
    """
    Plots the marker data on the current figure
    
    Parameters
    -----------------
    marker_data_df: DataFrame(grouped)
        Marker data to be plot on the current figure
    colors: ColorValues
        Color values to be cycled
    """
    
    marker_name = GetMarkerName(marker_data_df.marker_id.values[0])
    plt.plot(marker_data_df.ts.values,marker_data_df.measurement.values,'o-',color = colors[marker_data_df.index[0]%(len(colors)/2)*2],label = marker_name,lw = 1.5)
    
    
def PlotSiteMeas(surficial_data_df,site_id,ts):
    """
    Generates the measurements vs. time plot of the given surficial data
    
    Parameters
    ----------------
    surficial_data_df: DataFrame
        Data frame of the surficial data to be plot
    site_id: int
        Site id of the surficial data
        
    Returns
    ----------------
    None
    """
    #### Set output path
    plot_path = output_path+surficialconfig.io.surficial_meas_plots_path
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    
    #### Generate colors
    tableau20 = Tableau20Colors() 
    
    #### Get site code
    site_code = GetSiteCode(site_id)
    
    #### Initialize figure parameters
    plt.figure(figsize = (12,9))
    plt.grid(True)
    plt.suptitle("{} Measurement Plot for {}".format(site_code.upper(),pd.to_datetime(ts).strftime("%b %d, %Y %H:%M")),fontsize = 15)
    
    #### Group by markers
    marker_data_group = surficial_data_df.groupby('marker_id')

    #### Plot the measurement data of each marker
    marker_data_group.agg(PlotMarkerMeas,tableau20)
    
    #### Plot legend
    plt.legend(loc='upper left',fancybox = True, framealpha = 0.5)

    #### Rotate xticks
    plt.xticks(rotation = 45)
    
    #### Set xlabel and ylabel
    plt.xlabel('Timestamp', fontsize = 14)
    plt.ylabel('Measurement (cm)', fontsize = 14)
    
    plt.savefig(plot_path+"{} {} meas plot".format(site_code,pd.to_datetime(ts).strftime("%Y-%m-%d_%H-%M")),dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches = 'tight')


def PlotTrendingAnalysis(marker_id,date_time,time,displacement,time_array,disp_int,velocity,acceleration,velocity_data,acceleration_data):
    """
    Generates Trending plot given all parameters
    """
    #### Create output plot directory if it doesn't exists
    plot_path = output_path+surficialconfig.io.surficial_trending_plots_path
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    
    #### Generate Colors
    tableau20 = Tableau20Colors()
    
    #### Create figure
    fig = plt.figure()
    
    #### Set fig size
    fig.set_size_inches(15,8)
    
    #### Get marker details for labels
    site_code, marker_name = GetMarkerDetails(marker_id)
    
    #### Set fig title
    fig.suptitle('{} Marker {} {}'.format(site_code.upper(),marker_name,pd.to_datetime(date_time).strftime("%b %d, %Y %H:%M")),fontsize = 15)
    
    #### Set subplots for v-a, disp vs time, vel & acc vs time
    va_ax = fig.add_subplot(121)
    dvt_ax = fig.add_subplot(222)
    vt_ax = fig.add_subplot(224, sharex = dvt_ax)
    at_ax = vt_ax.twinx()
    #### Draw grid for all axis
    va_ax.grid()
    dvt_ax.grid()
    vt_ax.grid()
    
    #### Plot displacement vs. time and interpolation
    dvt_ax.plot(time,displacement,'.',color = tableau20[0],label = 'Data')    
    dvt_ax.plot(time_array,disp_int,color = tableau20[12],lw = 1.5,label = 'Interpolation')    
    
    #### Plot velocity vs time
    vt_ax.plot(time_array,velocity,color = tableau20[4],lw = 1.5,label = 'Velocity')
    
    #### Plot acceleration vs. time
    at_ax.plot(time_array,acceleration,color = tableau20[6],lw = 1.5,label = 'Acceleration')
    
    #### Resample velocity for plotting
    velocity_to_plot = np.linspace(min(velocity_data),max(velocity_data),1000)
    
    #### Compute for corresponding crit acceleration, and confidence interval to plot threshold line    
    acceleration_to_plot, acceleration_upper_bound, acceleration_lower_bound = ComputeCriticalAcceleration(velocity_to_plot)
    
    #### Plot threshold line
    threshold_line = va_ax.plot(velocity_to_plot,acceleration_to_plot,color = tableau20[0],lw = 1.5,label = 'Threshold Line')
    
    #### Plot confidence intervals
    va_ax.plot(velocity_to_plot,acceleration_upper_bound,'--',color = tableau20[0],lw = 1.5)
    va_ax.plot(velocity_to_plot,acceleration_lower_bound,'--',color = tableau20[0],lw = 1.5)
    
    #### Plot data points
    va_ax.plot(velocity_data,acceleration_data,color = tableau20[6],lw=1.5)
    past_points = va_ax.plot(velocity_data[1:],acceleration_data[1:],'o',color = tableau20[18],label = 'Past')
    current_point = va_ax.plot(velocity_data[0],acceleration_data[0],'*',color = tableau20[2],markersize = 9,label = 'Current')
    
    #### Set scale to log
    va_ax.set_xscale('log')    
    va_ax.set_yscale('log')
    
    #### Get all va-lines
    va_lines = threshold_line + past_points + current_point
        
    #### Set handler map
    h_map = {}
    for lines in va_lines[1:]:
        h_map[lines] = HandlerLine2D(numpoints = 1)
    
    #### Plot legends
    va_ax.legend(va_lines,[l.get_label() for l in va_lines],loc = 'upper left',handler_map = h_map,fancybox = True,framealpha = 0.5)
    dvt_ax.legend(loc = 'upper left',fancybox = True,framealpha = 0.5)
    vt_ax.legend(loc = 'upper left',fancybox = True,framealpha = 0.5)
    at_ax.legend(loc = 'upper right',fancybox = True,framealpha = 0.5)
    
    #### Plot labels
    va_ax.set_xlabel('velocity (cm/day)',fontsize = 14)
    va_ax.set_ylabel('acceleration (cm/day$^2$)',fontsize = 14)
    dvt_ax.set_ylabel('displacement (cm)',fontsize = 14)
    vt_ax.set_xlabel('time (days)',fontsize = 14)
    vt_ax.set_ylabel('velocity (cm/day)',fontsize = 14)
    at_ax.set_ylabel('acceleration (cm/day$^2$)',fontsize = 14)
    
    ### set file name
    filename = "{} {} {} trending plot".format(site_code,marker_name,pd.to_datetime(date_time).strftime("%Y-%m-%d_%H-%M"))
    
    #### Save fig
    plt.savefig(plot_path+filename,facecolor='w', edgecolor='w',orientation='landscape',mode='w',bbox_inches = 'tight')

def EvaluateTrendingFilter(marker_data_df,to_plot):
    """
    Function used to evaluate the Onset of Acceleration (OOA) Filter
    
    Parameters
    ---------------------
    marker_data_df: DataFrame
        Data for surficial movement for the marker
    to_plot: Boolean
        Determines if a trend plot will be generated
    
    Returns
    ----------------------
    trend_alert - 1 or 0
        1 -> passes the OOA Filter
        0 -> no significant trend detected by OOA Filter
    """
    
    #### Get time data in days zeroed from starting data point
    time = (marker_data_df.ts.values - marker_data_df.ts.values[-1])/np.timedelta64(1,'D')
    
    #### Get marker data in cm
    displacement = marker_data_df.measurement.values
    
    #### Get variance of gaussian weighted average for interpolation
    _,var = GaussianWeightedAverage(displacement)
    
    #### Compute for the spline interpolation and its derivative using the variance as weights
    spline = UnivariateSpline(time,displacement,w = 1/np.sqrt(var))
    spline_velocity = spline.derivative(n=1)
    spline_acceleration = spline.derivative(n=2)
    
    #### Resample time for plotting and confidence interval evaluation
    time_array = np.linspace(time[-1],time[0],1000)

    #### Compute for the interpolated displacement, velocity, and acceleration for data points using the computed spline
    disp_int = spline(time_array)    
    velocity = spline_velocity(time_array)
    acceleration = spline_acceleration(time_array)
    velocity_data = np.abs(spline_velocity(time))
    acceleration_data = np.abs(spline_acceleration(time))
        
    #### Compute for critical, upper and lower bounds of acceleration for the current data point
    crit_acceleration, acceleration_upper_threshold, acceleration_lower_threshold = ComputeCriticalAcceleration(np.abs(velocity[-1]))
    
    #### Trending alert = 1 if current acceleration is within the threshold and sign of velocity & acceleration is the same
    current_acceleration = np.abs(acceleration[-1])
    if current_acceleration <= acceleration_upper_threshold and current_acceleration >= acceleration_lower_threshold and velocity[-1]*acceleration[-1] > 0:
        trend_alert = 1
    else:
        trend_alert = 0
    
    #### Plot OOA trending analysis
    if to_plot:        
        PlotTrendingAnalysis(marker_data_df.marker_id.iloc[0],marker_data_df.ts.iloc[0],time,displacement,time_array,disp_int,velocity,acceleration,velocity_data,acceleration_data)
    
    return trend_alert

def EvaluateMarkerAlerts(marker_data_df,ts):
    """
    Function used to evaluates the alerts for every marker at a specified time
    
    Parameters
    -----------------
    marker_data_df: DataFrame
        Surficial data for the marker
    ts: Timestamp
        Timestamp for alert evaluation
    
    Returns
    -----------------
    marker_alerts_df: DataFrame
        DataFrame of marker alerts with columns [ts, marker_id,displacement,time_delta,alert_level]
    """

    #### Initialize values to zero to avoid reference before assignment error
    displacement = 0
    time_delta = 0
    
    #### Check if data is valid for given time of alert generation
    if RoundTime(marker_data_df.ts.values[0]) < RoundTime(ts):
        
        #### Marker alert is ND
        marker_alert = -1

    else:
        #### Surficial data is valid for time of release
    
        #### Check if data is sufficient for velocity computations
        if len(marker_data_df) < 2:
            
            #### Less than two data points, we assume a new marker, alert is L0
            marker_alert = 0
        
        else:
            #### Compute for time difference in hours
            time_delta = (marker_data_df.ts.iloc[0] - marker_data_df.ts.iloc[1])/np.timedelta64(1,'h')
            
            #### Compute for absolute displacement in cm
            displacement = np.abs(marker_data_df.measurement.iloc[0] - marker_data_df.measurement.iloc[1])
            
            #### Compute for velocity in cm/hour
            velocity = displacement / time_delta
            
            #### Check if submitted data exceeds reliability cap
            if displacement < 1:
                
                #### Displacement is less than 1 cm reliability cap, alert is L0
                marker_alert = 0
            else:
                
                #### Evaluate alert based on velocity alert table
                if velocity < surficialconfig.thresh.v_alert_2:
                    
                    #### Velocity is less than threshold velocity for alert 2, marker alert is L0
                    marker_alert = 0
                
                else:
                    #### Velocity if greater than threshold velocity for alert 2
                    
                    #### Check if there is enough data for trending analysis
                    if len(marker_data_df) < surficialconfig.io.surficial_num_pts:
                        #### Not enough data points for trending analysis
                        trend_alert = 1
                        
                    else:
                    #### Perform trending analysis
                        trend_alert = EvaluateTrendingFilter(marker_data_df,surficialconfig.io.PrintTrendPlot)
                    if velocity < surficialconfig.thresh.v_alert_3:
                        
                        #### Velocity is less than threshold for alert 3
                        ### If trend alert = 1, marker_alert = 2 -> L2 alert
                        ### If trend alert = 0, marker_alert = 1 -> L0t alert
                        marker_alert = 1 * trend_alert + 1
                    
                    else:
                        #### Velocity is greater than or equal to threshold for alert 3
                        marker_alert = 3
               
    return pd.Series({'ts':ts,'marker_id':int(marker_data_df.marker_id.iloc[0]),'displacement':displacement,'time_delta':time_delta,'alert_level':marker_alert})
    
#def marker_translation():
#    query = "SELECT markers.marker_id, marker_name FROM markers INNER JOIN marker_history ON marker_history.marker_id = markers.marker_id INNER JOIN marker_names ON marker_history.history_id = marker_names.history_id WHERE markers.site_id = 27"
#    df = q.GetDBDataFrame(query)
#    df['marker_name'] = df.marker_name.apply(lambda x:x[:1])
#    df.replace(to_replace = {'marker_name':{'C':81,'B':82,'E':83,'D':84}},inplace = True)
#    df.set_index('marker_id',inplace = True)
#    df = df.to_dict()['marker_name']
#    return df
#    
#def temp_fix(df):
#    marker_translation()
#    df['marker_id'] = df.marker_id.map(marker_translation())
#    return df

def GetTriggerSymID(alert_level):
    """
    Gets the corresponding trigger sym id given the alert level
    
    Parameters
    --------------
    alert_level: int
        surficial alert level
        
    Returns
    ---------------
    trigger_sym_id: int
        generated from operational_trigger_symbols table
    """
    #### query the translation table from operational_trigger_symbols table
    query = "SELECT trigger_sym_id,alert_level FROM operational_trigger_symbols WHERE trigger_source = 'surficial'"
    translation_table = q.GetDBDataFrame(query).set_index('alert_level').to_dict()['trigger_sym_id']
    return translation_table[alert_level]
    
def GetSurficialAlert(marker_alerts,site_id):
    """
    Generates the surficial alerts of a site given the marker alerts dataframe with the corresponding timestamp of generation 
    
    Parameters
    --------------
    marker_alerts: DataFrame
        Marker alerts dataframe with the following columns [ts,marker_id,displacement,time_delta,alert_level]
    site_id: int
        Site id of the generated marker alerts
    
    Returns
    ---------------
    surficial_alert: pd.DataFrame
        Series containing the surficial alert with columns [ts,site_id,trigger_sym_id, ts_updated]
    """
    
    #### Get the higher perceived risk from the marker alerts
    site_alert = max(marker_alerts.alert_level.values)
    
    #### Get the corresponding trigger sym id
    trigger_sym_id = GetTriggerSymID(site_alert)
    
    return pd.DataFrame({'ts':marker_alerts.ts.iloc[0],'site_id':site_id,'trigger_sym_id':trigger_sym_id,'ts_updated':marker_alerts.ts.iloc[0]},index = [0])

def GenerateSurficialAlert(site_id = None,ts = None):
    """
    Main alert generating function for surificial alert for a site at specified time
    
    Parameters
    ------------------
    site_id: int
        site_id of site of interest
    ts: timestamp
        timestamp of alert generation
        
    Returns
    -------------------
    Prints the generated alert and writes to marker_alerts database
    """
    #### Obtain system arguments from command prompt
    if site_id == None and ts == None:
        site_id, ts = sys.argv[1].lower(),sys.argv[2].lower()
    
    #### Config variables
    num_pts = int(surficialconfig.io.surficial_num_pts)
    
    #### Get latest ground data
    surficial_data_df = GetSurficialData(site_id,ts,num_pts)

    #### Generate Marker alerts
    marker_data_df = surficial_data_df.groupby('marker_id',as_index = False)
    marker_alerts = marker_data_df.apply(EvaluateMarkerAlerts,ts)
    
    #### Write to marker_alerts table    
    WriteToMarkerAlertsDB(marker_alerts)
    
    #### Generate surficial alert for site
    surficial_alert = GetSurficialAlert(marker_alerts,site_id)
    
    #### Write to db
    qdb.alert_toDB(surficial_alert,'operational_triggers')
    
    #### Plot current ground meas    
    if surficialconfig.io.PrintMeasPlot:
        
        #### Plot data with specified window
        ts_start = pd.to_datetime(ts) - pd.Timedelta(days = surficialconfig.io.meas_plot_window)
        
        ### Retreive the surficial data to plot
        surficial_data_to_plot = GetSurficialDataWindow(site_id,ts_start.strftime("%Y-%m-%d %H:%M"),ts)
        
        ### Plot the surficial data
        PlotSiteMeas(surficial_data_to_plot,site_id,ts)
    
    print marker_alerts
    return surficial_data_df

#Call the GenerateSurficialAlert() function
if __name__ == "__main__":
    GenerateSurficialAlert()  
    
    
