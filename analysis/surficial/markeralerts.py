##### IMPORTANT matplotlib declarations must always be FIRST
##### to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
from matplotlib.legend_handler import HandlerLine2D
import matplotlib.pyplot as plt
plt.ioff()

#### Import essential libraries
from datetime import datetime, time, timedelta
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
from scipy.interpolate import UnivariateSpline
from scipy.ndimage import filters
from scipy.signal import gaussian
import numpy as np
import os
import pandas as pd
import sys
import time as mytime

#### Import local codes
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import dynadb.db as db
import gsm.smsparser2.smsclass as sms

#### Open config files
sc = qdb.memcached()

#### Create directory
output_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                           '../../..'))

def gaussian_weighted_average(series, sigma = 3, width = 39):
    """ Computes for rolling weighted average and variance using
    a gaussian signal filter.
    
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
    b = gaussian(width, sigma)
    
    #### Take the rolling average using convolution
    average = filters.convolve1d(series, b/b.sum())
    
    #### Take the variance using convolution
    var = filters.convolve1d(np.power(series-average, 2), b/b.sum())
    
    return average, var


def round_time(date_time):
    """ Rounds given date_time to the next alert release time.
    
    """
    
    date_time = pd.to_datetime(date_time)
    time_hour = int(date_time.strftime('%H'))
    time_float = float(date_time.strftime('%H')) + \
                 float(date_time.strftime('%M'))/60

    quotient = time_hour // 4
    if quotient == 5:
        if time_float % 4 > 3.5:
            date_time = datetime.combine(date_time.date() + timedelta(1),
                                         time(4))
        else:
            date_time = datetime.combine(date_time.date() + timedelta(1),
                                         time(0))
    elif time_float % 4 > 3.5:
        date_time = datetime.combine(date_time.date(), time((quotient+2) * 4))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1) * 4))
            
    return date_time


def tableau_20_colors():
    """ Generates normalized RGB values of tableau20.
    
    """
    
    tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

    for i in range(len(tableau20)):    
        r, g, b = tableau20[i]    
        tableau20[i] = (r/255, g/255, b/255)
    return tableau20


def compute_confidence_interval_width(velocity):
    """ Computes for the width of the confidence interval for a given velocity.
    
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
    return float(sc['surficial']['ci_t_crit'])*np.sqrt(np.abs(1/(sc['surficial']['ci_n'])-2*sc['surficial']['ci_sum_res_square']*(1/sc['surficial']['ci_n'] + (np.log(np.abs(velocity)) - sc['surficial']['ci_v_log_mean'])**2/sc['surficial']['ci_var_v_log'])))


def compute_critical_acceleration(velocity):
    """ Computes for the critical acceleration and its lower and upper bounds
    for a given velocity range.
    
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
    crit_acceleration = np.exp(float(sc['surficial']['ci_slope'])*np.log(np.abs(velocity)) + float(sc['surficial']['ci_intercept']))
    
    #### Compute for confidence interval width width
    ci_width = compute_confidence_interval_width(velocity)
    
    #### Compute for lower and upper bound of acceleration
    acceleration_upper_bound = crit_acceleration*np.exp(ci_width)
    acceleration_lower_bound = crit_acceleration*np.exp(-ci_width)
    
    return crit_acceleration, acceleration_upper_bound, acceleration_lower_bound


def plot_marker_meas(marker_data_df, colors):
    """ Plots the marker data on the current figure.
    
    Parameters
    -----------------
    marker_data_df: DataFrame(grouped)
        Marker data to be plot on the current figure
    colors: ColorValues
        Color values to be cycled
        
    """
    
    marker_name = marker_data_df['marker_name'].values[0]
    plt.plot(marker_data_df.ts, marker_data_df.measurement, 'o-',
             color = colors[marker_data_df.index[0]%(len(colors)//2)*2],
             label = marker_name,lw = 1.5)
    
    
def plot_site_meas(surficial_data_df, ts):
    """ Generates the measurements vs. time plot of the given surficial data.
    
    Parameters
    ----------------
    surficial_data_df: DataFrame
        Data frame of the surficial data to be plot    
        
    """
    
    #### Set output path
    plot_path = output_path+sc['fileio']['surficial_meas_path']
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    
    #### Generate colors
    tableau20 = tableau_20_colors() 
    
    #### Get site code
    site_code = surficial_data_df['site_code'].values[0]
    
    #### Initialize figure parameters
    plt.figure(figsize = (12,9))
    plt.grid(True)
    plt.suptitle("{} Measurement Plot for {}".format(site_code.upper(),
                 pd.to_datetime(ts).strftime("%b %d, %Y %H:%M")), fontsize = 15)
    
    #### Group by markers
    marker_data_group = surficial_data_df.groupby('marker_id')

    #### Plot the measurement data of each marker
    marker_data_group.agg(plot_marker_meas, tableau20)
    
    #### Rearrange legend handles
    handles,labels = plt.gca().get_legend_handles_labels()
    handles = [i for (i, j) in sorted(zip(handles, labels),
                      key = lambda pair:pair[1])]    
    labels = sorted(labels)
    #### Plot legend
    plt.legend(handles, labels, loc='upper left', fancybox = True,
               framealpha = 0.5)

    #### Rotate xticks
    plt.xticks(rotation = 45)
    
    #### Set xlabel and ylabel
    plt.xlabel('Timestamp', fontsize = 14)
    plt.ylabel('Measurement (cm)', fontsize = 14)
    
    plt.savefig(plot_path+"{} {} meas plot".format(site_code,
                pd.to_datetime(ts).strftime("%Y-%m-%d_%H-%M")), dpi=160,
                facecolor='w', edgecolor='w', orientation='landscape', mode='w',
                bbox_inches = 'tight')


def plot_trending_analysis(site_code, marker_name, date_time, zeroed_time,
                           displacement, time_array, disp_int, velocity,
                           acceleration, velocity_data, acceleration_data):
    """ Generates Trending plot given all parameters.
    
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
    
    #### Set fig title
    fig.suptitle('{} Marker {} {}'.format(site_code.upper(), marker_name,
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
    dvt_ax.plot(zeroed_time, displacement, '.', color = tableau20[0],
                label = 'Data')    
    dvt_ax.plot(time_array ,disp_int, color = tableau20[12], lw = 1.5,
                label = 'Interpolation')
    
    #### Plot velocity vs time
    vt_ax.plot(time_array, velocity, color = tableau20[4], lw = 1.5,
               label = 'Velocity')
    
    #### Plot acceleration vs. time
    at_ax.plot(time_array, acceleration, color = tableau20[6], lw = 1.5,
               label = 'Acceleration')
    
    #### Resample velocity for plotting
    velocity_to_plot = np.linspace(min(velocity_data), max(velocity_data), 1000)
    
    #### Compute for corresponding crit acceleration, and confidence interval to plot threshold line    
    acceleration_to_plot, acceleration_upper_bound, acceleration_lower_bound = compute_critical_acceleration(velocity_to_plot)
    
    #### Plot threshold line
    threshold_line = va_ax.plot(velocity_to_plot, acceleration_to_plot,
                                color = tableau20[0], lw = 1.5,
                                label = 'Threshold Line')
    
    #### Plot confidence intervals
    va_ax.plot(velocity_to_plot, acceleration_upper_bound, '--',
               color = tableau20[0], lw = 1.5)
    va_ax.plot(velocity_to_plot, acceleration_lower_bound, '--',
               color = tableau20[0], lw = 1.5)
    
    #### Plot data points
    va_ax.plot(velocity_data, acceleration_data, color = tableau20[6], lw=1.5)
    past_points = va_ax.plot(velocity_data[1:], acceleration_data[1:], 'o',
                             color = tableau20[18],label = 'Past')
    current_point = va_ax.plot(velocity_data[0], acceleration_data[0], '*',
                               color = tableau20[2], markersize = 9,
                               label = 'Current')
    
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
    va_ax.legend(va_lines, [l.get_label() for l in va_lines], loc = 'upper left',
                            handler_map = h_map, fancybox = True, 
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
    filename = "{} {} {} trending plot".format(site_code, marker_name,
                pd.to_datetime(date_time).strftime("%Y-%m-%d_%H-%M"))
    
    #### Save fig
    plt.savefig(plot_path+filename, facecolor='w', edgecolor='w',
                orientation='landscape', mode='w', bbox_inches = 'tight')


def get_logspace_and_filter_nan(df):
    """ Function used to filter nan values and transform data to log space.
    
    """

    df = np.log(np.abs(df))
    df = df[~(np.isnan(df) | np.isinf(df))]
    
    return df


def evaluate_trending_filter(marker_data_df, to_plot, to_json=False):
    """ Function used to evaluate the Onset of Acceleration (OOA) Filter.
    
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
    
    data = marker_data_df[0:10].sort_values('ts')
    #### Get time data in days zeroed from starting data point
    zeroed_time = np.array((data['ts'] - data['ts'].values[0]) / np.timedelta64(1, 'D'))
    #### Get marker data in cm
    displacement = data['measurement'].values

    #### Get variance of gaussian weighted average for interpolation
    _,var = gaussian_weighted_average(displacement)
    
    #### Compute for the spline interpolation and its derivative using the variance as weights
    spline = UnivariateSpline(zeroed_time, displacement, w = 1/np.sqrt(np.abs(var)))
    spline_velocity = spline.derivative(n=1)
    spline_acceleration = spline.derivative(n=2)
    
    #### Resample time for plotting and confidence interval evaluation
    sample_num = 1000
    if to_json == True:
        sample_num = 20
    
    time_array = np.linspace(zeroed_time[0], zeroed_time[-1], sample_num)

    #### Compute for the interpolated displacement, velocity, and acceleration for data points using the computed spline
    disp_int = spline(time_array)
    velocity = spline_velocity(time_array)
    acceleration = spline_acceleration(time_array)
    velocity_data = np.abs(spline_velocity(zeroed_time))
    acceleration_data = np.abs(spline_acceleration(zeroed_time))

    #### Compute for critical, upper and lower bounds of acceleration for the current data point
    crit_acceleration, acceleration_upper_threshold, acceleration_lower_threshold = compute_critical_acceleration(np.abs(velocity[-1]))
    
    #### Trending alert = 1 if current acceleration is within the threshold and sign of velocity & acceleration is the same
    current_acceleration = np.abs(acceleration[-1])
    if current_acceleration <= acceleration_upper_threshold and current_acceleration >= acceleration_lower_threshold and velocity[0]*acceleration[0] > 0:
        trend_alert = 1
    else:
        trend_alert = 0
    
    #### Plot OOA trending analysis
    if to_plot:        
        plot_trending_analysis(data['site_code'].values[0],
                               data['marker_name'].values[0],
                               data['ts'].values[-1], zeroed_time,
                               displacement, time_array, disp_int, velocity,
                               acceleration, velocity_data, acceleration_data)
    
    if to_json:
        ts_list = pd.to_datetime(data['ts'].values)
        time_arr = pd.to_datetime(ts_list[0]) + np.array(list(map(lambda x: timedelta(x), time_array)))
        
        velocity_data = list(velocity_data)
        acceleration_data = list(acceleration_data)
        velocity_to_plot = np.linspace(min(velocity_data),
                                       max(velocity_data), 20)
        acceleration_to_plot, acceleration_upper_bound, acceleration_lower_bound = compute_critical_acceleration(velocity_to_plot)
        
        disp_int = disp_int[~(np.isnan(disp_int) | np.isinf(disp_int))]
        velocity = velocity[~(np.isnan(velocity) | np.isinf(velocity))]
        acceleration = acceleration[~(np.isnan(acceleration) | np.isinf(acceleration))]
        
        velocity_data = get_logspace_and_filter_nan(velocity_data)
        acceleration_data = get_logspace_and_filter_nan(acceleration_data)
        velocity_to_plot = get_logspace_and_filter_nan(velocity_to_plot)
        acceleration_to_plot = get_logspace_and_filter_nan(acceleration_to_plot)
        acceleration_upper_bound = get_logspace_and_filter_nan(acceleration_upper_bound)
        acceleration_lower_bound = get_logspace_and_filter_nan(acceleration_lower_bound)
        
        ts_list = list(map(lambda x: mytime.mktime(x.timetuple())*1000, ts_list))
        time_arr = list(map(lambda x: mytime.mktime(x.timetuple())*1000, time_arr))
        
        return_json = {'av': {'v':list(velocity_data),
                              'a':list(acceleration_data),
                              'v_threshold':list(velocity_to_plot),
                              'a_threshold_line':list(acceleration_to_plot),
                              'a_threshold_up':list(acceleration_upper_bound),
                              'a_threshold_down':list(acceleration_lower_bound)
                             },
                       'dvt': {'gnd': {'ts': ts_list,
                                       'surfdisp':list(displacement)
                                      },
                               'interp': {'ts': time_arr,
                                          'surfdisp':list(disp_int)
                                         }
                              },
                       'vat': {'v_n':list(velocity),
                               'a_n':list(acceleration),
                               'ts_n':list(time_arr)
                              }
                      }
        return return_json
    else:
        return trend_alert


def evaluate_marker_alerts(marker_data_df, ts):
    """ Function used to evaluates the alerts for every marker
    at a specified time.
    
    Parameters
    -----------------
    marker_data_df: DataFrame
        Surficial data for the marker
    ts: Timestamp
        Timestamp for alert evaluation
    
    Returns
    -----------------
    marker_alerts_df: DataFrame
        DataFrame of marker alerts with columns [ts, marker_id, displacement,
        time_delta, alert_level]
        
    """

    #### Initialize values to zero to avoid reference before assignment error
    displacement = np.nan
    time_delta = np.nan
    
    #### Check if data is valid for given time of alert generation
    if round_time(marker_data_df['ts'].values[0]) < round_time(ts):
        
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
            time_delta = (marker_data_df['ts'].values[0] - marker_data_df['ts'].values[1]) / np.timedelta64(1, 'h')
            
            #### Compute for absolute displacement in cm
            displacement = np.abs(marker_data_df['measurement'].values[0] - marker_data_df['measurement'].values[1])
            
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
               
    return pd.Series({'marker_id': int(marker_data_df.marker_id.iloc[0]),
                      'ts': ts, 'data_id': int(marker_data_df.data_id.iloc[0]),
                      'displacement': np.round(displacement, 1),
                      'time_delta': time_delta, 'alert_level': marker_alert})
    

def get_surficial_alert(marker_alerts, site_id):
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
    site_alert = max(marker_alerts.alert_level)
    
    #### Get the corresponding trigger sym id
    trigger_sym_id = qdb.get_trigger_sym_id(site_alert)
    
    return pd.DataFrame({'ts':marker_alerts.ts.iloc[0], 'site_id':site_id,
                         'trigger_sym_id':trigger_sym_id,
                         'ts_updated':marker_alerts.ts.iloc[0]}, index = [0])


def generate_surficial_alert(site_id = None,ts = None):
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
    num_pts = int(sc['surficial']['surficial_num_pts'])
    ts_start = pd.to_datetime(ts) - timedelta(sc['surficial']['meas_plot_window'])

    #### Get latest ground data
    surficial_data_df = qdb.get_surficial_data(site_id, ts_start, ts, num_pts)
    
    #### Generate Marker alerts
    marker_data_df = surficial_data_df.groupby('marker_id',as_index = False)
    marker_alerts = marker_data_df.apply(evaluate_marker_alerts, ts)

    #### Write to marker_alerts table    
    data_table = sms.DataTable('marker_alerts', marker_alerts)
    db.df_write(data_table)

    #### Generate surficial alert for site
    surficial_alert = get_surficial_alert(marker_alerts,site_id)
    #### Write to db
    qdb.alert_to_db(surficial_alert,'operational_triggers')
    
    #### Plot current ground meas    
    if sc['surficial']['print_meas_plot']:
        ### Retreive the surficial data to plot
        surficial_data_to_plot = surficial_data_df.loc[surficial_data_df.ts >= ts_start, :]
        ### Plot the surficial data
        plot_site_meas(surficial_data_to_plot, ts)
    
    return surficial_data_df


#Call the generate_surficial_alert() function
if __name__ == "__main__":
    start = datetime.now()
    surficial_data_df = generate_surficial_alert(18, '2017-10-20 15:10:00')
    print ('runtime =', datetime.now()-start)