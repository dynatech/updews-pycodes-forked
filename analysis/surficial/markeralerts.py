import os
import re
import pandas as pd
import numpy as np
import analysis.querydb as qdb
import matplotlib as mpl
mpl.use('Agg')
from datetime import datetime, time, timedelta
import time as mytime
from scipy.interpolate import UnivariateSpline
from scipy.signal import gaussian
from scipy.ndimage import filters
from sqlalchemy import create_engine
import volatile.memory as mem
import dynadb.db as db
import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerLine2D
import platform

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver
sc = mem.server_config()
output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))


def gaussian_weighted_average(series,sigma=3,width=39):

    """
    - Computes for rolling weighted average and variance using a gaussian signal filter.

    Args:
        series (Array): Series to be averaged.
        sigma (Float): Standard deviation of the gaussian filter
        Width (int): Number of points to create the gaussian filter

    Returns:
        average (Array): Rolling weighted average of the series.
        var (Array): Rolling variance of the series
    """   

    #### Create the Gaussian Filter
    b = gaussian(width,sigma)
    
    #### Take the rolling average using convolution
    average = filters.convolve1d(series, b/b.sum())
    
    #### Take the variance using convolution
    var = filters.convolve1d(np.power(series-average,2), b/b.sum())
    
    return average, var


def round_time(date_time):

    """
    - Rounds given date_time to the next alert release time.

    Args:
        date_time (str): Date Time.

    """  
    date_time = pd.to_datetime(date_time)
    time_hour = int(date_time.strftime('%H'))
    time_float = (float(date_time.strftime('%H')) + 
                  float(date_time.strftime('%M')) /60)
                  
    quotient = time_hour / 4
    
    if quotient == 5:
        if time_float % 4 > 3.5:
            date_time = datetime.combine(date_time.date() + timedelta(1), 
                                         time(4,0,0))
        else:
            date_time = datetime.combine(date_time.date() + timedelta(1), 
                                         time(0,0,0))
    elif time_float % 4 > 3.5:
        date_time = datetime.combine(date_time.date(), 
                                     time((quotient + 2)*4,0,0))
    else:
        date_time = datetime.combine(date_time.date(), 
                                     time((quotient + 1)*4,0,0))
            
    return date_time
    

def tableau_20_colors():

    """
    - Generates normalized RGB values of tableau20.

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


def compute_critical_acceleration(velocity):

    """
    - Computes for the critical acceleration and its lower and upper bounds for a given velocity range.

    Args:
        velocity (Array): velocity of interest.

    Returns:
        compute_critical_acceleration (dict): Rolling weighted average of the series.
            crit_acceleration: array-like same size as input
                - corresponding critical acceleration for each given velocity
            acceleration_upper_bound: array-like
                - upper bound for acceleration
            acceleration_lower_bound: array-like
                -lower bound for acceleration
    """  

    #### Compute for critical acceleration from computed slope and intercept from critical values
    crit_acceleration = (np.exp(float(sc['surficial']['ci_slope'])*np.log(velocity) +
                        float(sc['surficial']['ci_intercept'])))
    
    #### Compute for confidence interval width width
    ci_width = float(sc['surficial']['ci_t_crit'])*np.sqrt(1/(float(
               sc['surficial']['ci_n'])-2)*float(sc['surficial']
               ['ci_sum_res_square'])*(1/float(sc['surficial']['ci_n']) + 
               (np.log(velocity) - float(sc['surficial']['ci_v_log_mean']))
               **2/float(sc['surficial']['ci_var_v_log'])))

    
    #### Compute for lower and upper bound of acceleration
    acceleration_upper_bound = crit_acceleration*np.exp(ci_width)
    acceleration_lower_bound = crit_acceleration*np.exp(-ci_width)
    
    return {'crit_acceleration':crit_acceleration, 
            'acceleration_upper_bound':acceleration_upper_bound, 
            'acceleration_lower_bound':acceleration_lower_bound} 
            

def reset_auto_increment():

    """
    - Reset autoincrement to maximum value after delete

    """

    #### Connect to db
    db = mysqlDriver.connect(host = sc['hosts']['local'], 
                             user = sc['db']['user'], 
                             passwd = sc['db']['password'])
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(sc['db']['name']))

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
    
    
def delete_duplicates_marker_alerts_db(marker_alerts_df):

    """
    - Deletes entries on the database with the same timestamp and marker id as the supplied marker alerts df.

    Args:
        marker_alerts_df (dataframe): Marker alerts dataframe with columns: [ts, marker_id, displacement, time_delta, alert_level].

    """ 

    #### Collect the values to be deleted
    values_to_delete = zip(marker_alerts_df.ts.values, 
                           marker_alerts_df.marker_id.values)
    
    #### Connect to db
    db = mysqlDriver.connect(host = sc['hosts']['local'], 
                             user = sc['db']['user'], 
                             passwd = sc['db']['password'])
    
    #### Initialize cursor    
    cur = db.cursor()
    
    #### Use default database
    cur.execute("USE {}".format(sc['db']['name']))
    
    #### Iterate cur.execute to delete specified values
    for ts,marker_id in values_to_delete:
        #### Create query
        query = query_pattern(pattern_id="delete_marker_id", 
                              dictionary={'ts':ts,'marker_id':marker_id})
        cur.execute(query)
    
    #### Commit changes 
    db.commit()

    #### Close db
    db.close()


def write_to_marker_alerts_db(marker_alerts_df):

    """
    - Writes the input marker alerts to the database, replacing any duplicate entries.

    Args:
        marker_alerts_df (dataframe): Marker alerts dataframe with columns: [ts, marker_id, displacement, time_delta, alert_level].

    """ 

    marker_alerts_df['ts'] = map(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M'), 
                            marker_alerts_df.ts.values)
    #### Delete possible duplicates
                                    
    delete_duplicates_marker_alerts_db(marker_alerts_df)
    
    
    ### Reset the auto increment
    reset_auto_increment()
    
    #### Create engine to connect to db
    engine=create_engine('mysql://'+sc['db']['user']+':'+sc['db']['password']+
                        '@'+sc['hosts']['local']+':3306/'+sc['db']['name'])
    
    #### Insert dataframe to the database
    marker_alerts_df.set_index('ts').to_sql(name = 'marker_alerts', 
                                            con = engine, 
                                            if_exists = 'append', 
                                            schema = sc['db']['name'], 
                                            index = True)


def plot_marker_meas(marker_data_df,colors):

    """
    - Plots the marker data on the current figure.

    Args:
        marker_alerts_df (dataframe): Marker data to be plot on the current figure
        color (str): Color values to be cycled
    """  

    marker_name = query_pattern(pattern_id="marker_name",
                                output_type="read",
                                dictionary={"marker_id": str(marker_data_df.marker_id.values[0])})
                                
    marker_name = marker_name["marker_name"][0]
    plt.plot(marker_data_df.ts.values,
             marker_data_df.measurement.values,
             'o-',
             color = colors[marker_data_df.index[0]%(len(colors)/2)*2],
            label = marker_name,lw = 1.5)
    
    
def plot_site_meas(surficial_data_df,site_id,ts):

    """
    - Generates the measurements vs. time plot of the given surficial data.

    Args:
        surficial_data_df (dataframe): Data frame of the surficial data to be plot.
        site_id (int): Standard deviation of the gaussian filter

    """  

    #### Set output path
    plot_path = output_path+sc['fileio']['surficial_meas_path']
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    
    #### Generate colors
    tableau20 = tableau_20_colors() 
    
    #### Get site code
    site_code = query_pattern(pattern_id="site_code", 
                              output_type="read",
                              dictionary={"site_id":str(site_id)})
    site_code = site_code["site_code"][0]   
                
    #### Initialize figure parameters
    plt.figure(figsize = (12,9))
    plt.grid(True)
    plt.suptitle("{} Measurement Plot for {}".format(site_code.upper(), 
                pd.to_datetime(ts).strftime("%b %d, %Y %H:%M")),
                fontsize = 15)
    
    #### Group by markers
    marker_data_group = surficial_data_df.groupby('marker_id')

    #### Plot the measurement data of each marker
    marker_data_group.agg(plot_marker_meas, tableau20)
    
    #### Rearrange legend handles
    handles,labels = plt.gca().get_legend_handles_labels()
    handles = [i for (i,j) in sorted(zip(handles,labels), key = lambda pair:pair[1])]    
    labels = sorted(labels)
    #### Plot legend
    plt.legend(handles, labels, loc='upper left', fancybox = True, framealpha = 0.5)

    #### Rotate xticks
    plt.xticks(rotation = 45)
    
    #### Set xlabel and ylabel
    plt.xlabel('Timestamp', fontsize = 14)
    plt.ylabel('Measurement (cm)', fontsize = 14)
    
    plt.savefig(plot_path+"{} {} meas plot".format(site_code, 
                pd.to_datetime(ts).strftime("%Y-%m-%d_%H-%M")),
                dpi=160, facecolor='w', edgecolor='w', 
                orientation='landscape', mode='w', bbox_inches = 'tight')



def plot_trending_analysis(marker_id ,date_time, time, displacement, 
                           time_array, disp_int, velocity, acceleration, 
                           velocity_data, acceleration_data):

    """
    - Generates Trending plot given all parameters
    """

    #### Create output plot directory if it doesn't exists
    plot_path = output_path+sc['fileio']['surficial_trending_path']
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    
    #### Generate Colors
    tableau20 = tableau_20_colors()
    
    #### Create figure
    fig = plt.figure()
    
    #### Set fig size
    fig.set_size_inches(15,8)
    
    #### Get marker details for labels
    site_code, marker_name = query_pattern(pattern_id="marker_details",
                                           output_type="read",
                                           dictionary={"marker_id": str(marker_id)})
    
    #### Set fig title
    fig.suptitle('{} Marker {} {}'.format(site_code.upper(),
                                          marker_name,
                                          pd.to_datetime(date_time).strftime("%b %d, %Y %H:%M")),
                                          fontsize = 15)
    
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
    compute_acceleration = compute_critical_acceleration(velocity_to_plot)
    #### Plot threshold line
    threshold_line = va_ax.plot(velocity_to_plot, 
                                compute_acceleration['crit_acceleration'], 
                                color = tableau20[0], 
                                lw = 1.5, 
                                label = 'Threshold Line')
    
    #### Plot confidence intervals
    va_ax.plot(velocity_to_plot, 
               compute_acceleration['acceleration_upper_bound'], 
               '--', 
               color = tableau20[0], 
                lw = 1.5)
                
    va_ax.plot(velocity_to_plot, 
               compute_acceleration['acceleration_lower_bound'],
               ' --', 
               color = tableau20[0], 
               lw = 1.5)
    
    #### Plot data points
    va_ax.plot(velocity_data, acceleration_data, color = tableau20[6], lw=1.5)
    past_points = va_ax.plot(velocity_data[1:], 
                             acceleration_data[1:], 
                             'o', color = tableau20[18], 
                             label = 'Past')
    current_point = va_ax.plot(velocity_data[0],
                               acceleration_data[0],
                               '*',color = tableau20[2],
                               markersize = 9,label = 'Current')
    
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
    va_ax.legend(va_lines,[l.get_label() for l in va_lines], 
                           loc = 'upper left', 
                           handler_map = h_map, 
                           fancybox = True, 
                           framealpha = 0.5)

    dvt_ax.legend(loc = 'upper left', fancybox = True, framealpha = 0.5)
    vt_ax.legend(loc = 'upper left', fancybox = True, framealpha = 0.5)
    at_ax.legend(loc = 'upper right', fancybox = True, framealpha = 0.5)
    
    #### Plot labels
    va_ax.set_xlabel('velocity (cm/day)', fontsize = 14)
    va_ax.set_ylabel('acceleration (cm/day$^2$)', fontsize = 14)
    dvt_ax.set_ylabel('displacement (cm)', fontsize = 14)
    vt_ax.set_xlabel('time (days)', fontsize = 14)
    vt_ax.set_ylabel('velocity (cm/day)', fontsize = 14)
    at_ax.set_ylabel('acceleration (cm/day$^2$)', fontsize = 14)
    
    ### set file name
    filename = "{} {} {} trending plot".format(site_code, 
                marker_name, 
                pd.to_datetime(date_time).strftime("%Y-%m-%d_%H-%M"))
    
    #### Save fig
    plt.savefig(plot_path+filename,facecolor='w', 
                edgecolor='w',
                orientation='landscape',
                mode='w',
                bbox_inches = 'tight')



def get_logspace_and_filter_nan(df):
    
    """
    - Log space and Filter nan data.

    Args:
        df (Dataframe): Unfiltered Dataframe.

    Returns:
        df (Dataframe): Filtered dataframe and logspace.
    """   
    df = np.log(df)
    df = df[~np.logical_or(np.isnan(df), np.isinf(df))]
    
    return df


def evaluate_trending_filter(marker_data_df,to_plot,to_json=False):


    """
    - Function used to evaluate the Onset of Acceleration (OOA) Filter.

    Args:
        marker_data_df (dataframe): Data for surficial movement for the marker.
        to_plot (Boolean): Determines if a trend plot will be generated.
        to_json (Boolean): Generate JSON output.

    Returns:
        trend_alert (Boolean): trend_alert - 1 or 0. 
            1 -> passes the OOA Filter
            0 -> no significant trend detected by OOA Filter
    """   

    #### Get time data in days zeroed from starting data point
    time = (marker_data_df.ts.values - marker_data_df.ts.values[-1])/np.timedelta64(1,'D')
    
    #### Get marker data in cm
    displacement = marker_data_df.measurement.values
    
    #### Get variance of gaussian weighted average for interpolation
    _,var = gaussian_weighted_average(displacement)
    
    #### Compute for the spline interpolation and its derivative using the variance as weights
    spline = UnivariateSpline(time,displacement, w = 1/np.sqrt(var))
    spline_velocity = spline.derivative(n=1)
    spline_acceleration = spline.derivative(n=2)
    
    #### Resample time for plotting and confidence interval evaluation
    sample_num = 1000
    if to_json == True:
        sample_num = 20
    
    time_array = np.linspace(time[-1], time[0], sample_num)

    #### Compute for the interpolated displacement, velocity, and acceleration for data points using the computed spline
    disp_int = spline(time_array)
    velocity = spline_velocity(time_array)
    acceleration = spline_acceleration(time_array)
    velocity_data = np.abs(spline_velocity(time))
    acceleration_data = np.abs(spline_acceleration(time))

    #### Compute for critical, upper and lower bounds of acceleration for the current data point

    compute_acceleration = compute_critical_acceleration(np.abs(velocity[-1]))
    #### Trending alert = 1 if current acceleration is within the threshold and sign of velocity & acceleration is the same
    current_acceleration = np.abs(acceleration[-1])
    if (current_acceleration <= compute_acceleration['acceleration_upper_threshold'] and 
        current_acceleration >= compute_acceleration['acceleration_lower_threshold'] and
        velocity[-1]*acceleration[-1] > 0):
        
        trend_alert = 1
    else:
        trend_alert = 0
    
    #### Plot OOA trending analysis
    if to_plot:        
        plot_trending_analysis(marker_data_df.marker_id.iloc[0], 
                               marker_data_df.ts.iloc[0], 
                               time,
                               displacement,
                               time_array,
                               disp_int,
                               velocity,
                               acceleration,
                               velocity_data,
                               acceleration_data)
    
    if to_json:
        ts_list = pd.to_datetime(marker_data_df.ts.values)
        time_arr = pd.to_datetime(ts_list[-1]) + np.array(map(lambda x: timedelta(days = x), time_array))
        
        velocity_data = list(reversed(velocity_data))
        acceleration_data = list(reversed(acceleration_data))
        velocity_to_plot = np.linspace(min(velocity_data),max(velocity_data),20)
        compute_acceleration = compute_critical_acceleration(velocity_to_plot)

        
        disp_int = disp_int[~np.logical_or(np.isnan(disp_int),np.isinf(disp_int))]
        velocity = velocity[~np.logical_or(np.isnan(velocity),np.isinf(velocity))]
        acceleration = acceleration[~np.logical_or(np.isnan(acceleration),np.isinf(acceleration))]
        
        velocity_data = get_logspace_and_filter_nan(velocity_data)
        acceleration_data = get_logspace_and_filter_nan(acceleration_data)
        velocity_to_plot = get_logspace_and_filter_nan(velocity_to_plot)
        acceleration_to_plot = get_logspace_and_filter_nan(compute_acceleration['crit_acceleration'])
        acceleration_upper_bound = get_logspace_and_filter_nan(compute_acceleration['acceleration_upper_bound'])
        acceleration_lower_bound = get_logspace_and_filter_nan(compute_acceleration['acceleration_lower_bound'])
        
        ts_list = map(lambda x: mytime.mktime(x.timetuple())*1000, ts_list)
        time_arr = map(lambda x: mytime.mktime(x.timetuple())*1000, time_arr)
        
        return_json = {'av':{'v':list(velocity_data), 
                             'a':list(acceleration_data),
                             'v_threshold':list(velocity_to_plot),
                             'a_threshold_line':list(acceleration_to_plot),
                             'a_threshold_up':list(acceleration_upper_bound),
                             'a_threshold_down':list(acceleration_lower_bound)},
                             'dvt':{'gnd':{'ts':list(ts_list),
                             'surfdisp':list(displacement)},
                             'interp':{'ts':list(time_arr),
                             'surfdisp':list(disp_int)}},
                             'vat':{'v_n':list(velocity),
                             'a_n':list(acceleration),
                             'ts_n':list(time_arr)}}
        return return_json
    else:
        return trend_alert


def evaluate_marker_alerts(marker_data_df, ts):
    
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
    if round_time(marker_data_df.ts.values[0]) < round_time(ts):
        
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
            time_delta = ((marker_data_df.ts.iloc[0] - marker_data_df.ts.iloc[1])
                            /np.timedelta64(1,'h'))
            
            #### Compute for absolute displacement in cm
            displacement = np.abs(marker_data_df.measurement.iloc[0] - 
                            marker_data_df.measurement.iloc[1])
            
            #### Compute for velocity in cm/hour
            velocity = displacement / time_delta
            
            #### Check if submitted data exceeds reliability cap
            if displacement < 1:
                
                #### Displacement is less than 1 cm reliability cap, alert is L0
                marker_alert = 0
            else:
                
                #### Evaluate alert based on velocity alert table
                if velocity < float(sc['surficial']['v_alert_2']):
                    
                    #### Velocity is less than threshold velocity for alert 2, marker alert is L0
                    marker_alert = 0
                
                else:
                    #### Velocity if greater than threshold velocity for alert 2
                    
                    #### Check if there is enough data for trending analysis
                    if len(marker_data_df) < int(sc['surficial']['surficial_num_pts']):
                        #### Not enough data points for trending analysis
                        trend_alert = 1
                        
                    else:
                    #### Perform trending analysis
                        trend_alert = evaluate_trending_filter(marker_data_df, 
                                                               sc['surficial']['print_trend_plot'])
                    if velocity < float(sc['surficial']['v_alert_3']):
                        
                        #### Velocity is less than threshold for alert 3
                        ### If trend alert = 1, marker_alert = 2 -> L2 alert
                        ### If trend alert = 0, marker_alert = 1 -> L0t alert
                        marker_alert = 1 * trend_alert + 1
                    
                    else:
                        #### Velocity is greater than or equal to threshold for alert 3
                        marker_alert = 3
               
    return pd.Series({'ts':ts,'marker_id':int(marker_data_df.marker_id.iloc[0]),
                      'displacement':displacement,
                      'time_delta':time_delta,
                      'alert_level':marker_alert})
    
    
def get_surficial_alert(marker_alerts,site_id):


    """
    - Generates the surficial alerts of a site given the marker alerts dataframe with the corresponding timestamp of generation.

    Args:
        marker_alerts (dataframe): Marker alerts dataframe with the following columns [ts,marker_id,displacement,time_delta,alert_level].
        site_id (int): Site id of the generated marker alerts.

    Returns:
        surficial_alert (dataframe): Series containing the surficial alert with columns [ts,site_id,trigger_sym_id, ts_updated]. 
            
    """ 

    #### Get the higher perceived risk from the marker alerts
    site_alert = max(marker_alerts.alert_level.values)
    
    #### Get the corresponding trigger sym id
    query = query_pattern(pattern_id="trigger_sym_id")
    translation_table = qdb.get_db_dataframe(query).set_index('alert_level')
    translation_table = translation_table.to_dict()['trigger_sym_id']

    trigger_sym_id = translation_table[site_alert]
    
    return pd.DataFrame({'ts':marker_alerts.ts.iloc[0],
    'site_id':site_id,'trigger_sym_id':trigger_sym_id,
    'ts_updated':marker_alerts.ts.iloc[0]}, index = [0])


def query_pattern(pattern_id="",dictionary="",output_type=""):

    """
    - Query list of all query use in markeralerts.

    Args:
        pattern_id (str): Query pattern id.
        dictionary (dict): Dictionary of all inputs in the pattern.
        output_type (str): Input "read" to output dataframe.

    Returns:
        return (dataframe, str): Returns query string or dataframe output from database. 
            
    """ 
    
    if pattern_id == "trigger_sym_id":
        query =("SELECT trigger_sym_id, alert_level FROM "
                "  operational_trigger_symbols AS op INNER JOIN "
                "  (SELECT source_id FROM trigger_hierarchies "
                "  WHERE trigger_source = 'surficial' "
                "  ) AS trig ON op.source_id = trig.source_id")
    elif pattern_id == "marker_name":
        query = ("SELECT marker_name FROM marker_history INNER JOIN"
                " markers ON markers.marker_id = marker_history.marker_id "
                "INNER JOIN marker_names ON marker_history.history_id = "
                "marker_names.history_id WHERE marker_history.history_id = "
                "(SELECT max(history_id) FROM marker_history WHERE event in "
                "('add','rename') AND marker_id = [marker_id])")
    elif pattern_id == "delete_marker_id":
        query = ("DELETE FROM marker_alerts WHERE ts = '[ts]' AND "
                  "marker_id = [marker_id] ")       
    elif pattern_id == "alter_marker_alert":
        query = ("ALTER TABLE marker_alerts AUTO_INCREMENT = [AUTO_INCREMENT]")      
    elif pattern_id == "alter_marker_alert":
        query = ("SELECT max(ma_id) FROM marker_alerts")
    elif pattern_id == "create_marker_alerts":
        query = ("CREATE TABLE IF NOT EXISTS marker_alerts(ma_id int"
                 " AUTO_INCREMENT, ts timestamp, marker_id smallint(6) "
                 "unsigned, displacement float, time_delta float, alert_level "
                 "tinyint, PRIMARY KEY (ma_id), FOREIGN KEY (marker_id) "
                 "REFERENCES markers(marker_id))")
    elif pattern_id == "surficial_data":
        query = ("SELECT mo1.ts, md1.marker_id, md1.measurement, COUNT(*)"
                 " num FROM marker_data md1 INNER JOIN marker_data md2 "
                 "INNER JOIN marker_observations mo1 INNER JOIN "
                 "marker_observations mo2 ON mo1.mo_id = md1.mo_id AND "
                 "mo2.mo_id = mo2.mo_id AND mo1.site_id = mo2.site_id AND "
                 "md1.marker_id = md2.marker_id AND md1.data_id = md2.data_id"
                 " AND mo1.ts <= mo2.ts AND mo1.ts <= '[ts]' AND "
                 "mo2.ts <= '[ts]' AND md1.marker_id in (SELECT marker_id "
                 "FROM marker_data INNER JOIN marker_observations ON "
                 "marker_data.mo_id = marker_observations.mo_id AND "
                 "site_id = [site_id] WHERE ts = "
                 "(SELECT max(ts) FROM marker_observations WHERE site_id = "
                 "[site_id] AND ts <= '[ts]')) GROUP BY md1.marker_id, "
                 "md1.mo_id, mo1.site_id, mo1.mo_id,mo1.ts HAVING "
                 "COUNT(*) <= [count] ORDER by mo1.ts DESC")    
    elif pattern_id == "surficial_data_window":
        query = ("SELECT ts, marker_id, measurement FROM marker_data INNER"
                 " JOIN marker_observations ON marker_observations.mo_id "
                 "= marker_data.mo_id AND ts <= '[ts_2]' AND ts >= '[ts_1]' "
                 "AND marker_id in (SELECT marker_id FROM marker_data INNER "
                 "JOIN marker_observations ON marker_data.mo_id = "
                 "marker_observations.mo_id WHERE site_id = [site_id] AND "
                 "ts = (SELECT max(ts) FROM marker_observations WHERE "
                 "site_id = [site_id] AND ts <= '[ts_1]')) ORDER by ts DESC")
    elif pattern_id == "site_code":
        query = ("SELECT site_code FROM sites WHERE site_id = '[site_id]'")
    elif pattern_id == "marker_details":
        query = ("SELECT site_code,marker_name FROM marker_history INNER "
                "JOIN markers ON markers.marker_id = marker_history.marker_id "
                "INNER JOIN sites ON markers.site_id = sites.site_id INNER "
                "JOIN marker_names ON marker_history.history_id = "
                "marker_names.history_id WHERE marker_history.history_id = "
                "(SELECT max(history_id) FROM marker_history WHERE event in "
                "('add','rename') AND marker_id = [marker_id]")
    else:
         raise ValueError("template_id doesn't exists")
         return
         
    if dictionary:  
        for item in sorted(dictionary.keys()):
            query = (re.sub(r'\[' + item + '\]', dictionary[item], query))

    if output_type == "read":
        return db.df_read(query=query)

    else:
        return query
            

def generate_surficial_alert(site_id="",ts="",output_type="graph",write=""):


    """
    - Main alert generating function for surificial alert for a site at specified time.

    Args:
        site_id (str): Site_id of site of interest.
        ts (date): Timestamp of alert generation .
        output_type (str): Type of output.Default to graph
        write (str): Enable write to database.

    Returns:
        return (dataframe, graph): Prints the generated alert and writes to marker_alerts database. 
          
    """


    if all([site_id, ts]):
       
        #### Config variables
        num_pts = int(sc['surficial']['surficial_num_pts'])
        
        #### Get latest ground data
        try:
            surficial_data_df = query_pattern(pattern_id="surficial_data",
                                              output_type="read",
                                              dictionary={"site_id":str(site_id), 
                                                          "ts": str(ts),
                                                          "count":str(num_pts)})
                                                           
        except:
            raise ValueError(" Input the right values")
            
        #### Generate Marker alerts
        marker_data_df = surficial_data_df.groupby('marker_id', 
                                                   as_index = False)
        marker_alerts = marker_data_df.apply(evaluate_marker_alerts, ts)
        
        #### Generate surficial alert for site
   
        surficial_alert = get_surficial_alert(marker_alerts,site_id)
        
        if write:
            #### Write to marker_alerts table   
            write_to_marker_alerts_db(marker_alerts) 
            
            #### Write to db
            try:
                qdb.alert_to_db(surficial_alert,'operational_triggers')
                print "Write operational_triggers"
                
            except:
                raise ValueError(" Error in writing operational_triggers")
                
        if output_type == "dataframe":
            return surficial_data_df
            
        elif output_type == "graph":
            #### Plot current ground meas    
            #### Plot data with specified window
            ts_start = pd.to_datetime(ts) - pd.Timedelta(days = int(
            sc['surficial']['meas_plot_window']))
            
            ### Retreive the surficial data to plot
            surficial_data_to_plot = query_pattern(
                                     pattern_id="surficial_data_window", 
                                     output_type="read",
                                     dictionary={"site_id":str(site_id),
                                     "ts_1":ts_start.strftime("%Y-%m-%d %H:%M"),
                                     "ts_2": ts}) 
            ### Plot the surficial data
            plot_site_meas(surficial_data_to_plot,site_id,ts)
        
    else:
       raise ValueError(" Input the right values")


if __name__ == "__main__":
    generate_surficial_alert()  
    
    
