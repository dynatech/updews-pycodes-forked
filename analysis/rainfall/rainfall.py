from datetime import datetime, timedelta, date, time
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import rainfallalert as ra
import rainfallplot as rp
import volatile.memory as mem

############################################################
##      TIME FUNCTIONS                                    ##    
############################################################

def get_rt_window(rt_window_length, roll_window_length, end=datetime.now()):
    """Rounds time to 4/8/12 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off. 04:00 to 07:30 is
        rounded off to 8:00, 08:00 to 11:30 to 12:00, etc.

    Returns:
        datetime: Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    
    ##OUTPUT: 
    ##end, start, offsetstart;
    ##datetimes; dates for the end, start and offset start of the 
    ##real-time monitoring window 

    ##round down current time to the nearest HH:00 or HH:30 time value
    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year, end_month, end_day),
                         time(end_hour, end_minute, 0))

    #starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+roll_window_length)
    
    return end, start, offsetstart



def rainfall_gauges(end=datetime.now()):
    """Check top 4 rain gauges to be used in rainfall analysis.
    
    Args:
        end (datetime): Timestamp of alert and plot to be computed. Optional.
                        Defaults to current timestamp.

    Returns:
        dataframe: Top 4 rain gauges per site.
    
    """

    gauges = mem.get('df_rain_props')

    gauges['gauge_name'] = np.array(','.join(gauges.data_source).replace('noah',
                                 'rain_noah_').replace('senslope',
                                 'rain_').split(','))+gauges.gauge_name
    gauges = gauges.sort_values('distance')
    gauges = gauges.groupby('site_id').head(4)
    gauges = gauges.sort_values(['site_id', 'distance'])

    return gauges

def web_plotter(site_code, end, days):
    """
    Created by Kevin
    For integration and refactoring in the future
    """

    start_time = datetime.now()
    qdb.print_out(start_time)

    site_code = [site_code]
    dt_end = pd.to_datetime(end)
    sc = mem.server_config()
    rtw = sc['rainfall']['roll_window_length']
    ts_end, start, offsetstart = get_rt_window(
        float(days), float(rtw), end=dt_end)

    gauges = rainfall_gauges()
    if site_code != '':
        gauges = gauges[gauges.site_code.isin(site_code)]

    gauges['site_id'] = gauges['site_id'].apply(lambda x: float(x))
    site_props = gauges.groupby('site_id')

    plot_data = site_props.apply(rp.main, offsetstart=offsetstart,
                                 tsn=end, save_plot=False, sc=sc,
                                 start=start, output_path="",
                                 end=ts_end).reset_index(drop=True)

    json_plot_data = plot_data.to_json(orient="records")

    qdb.print_out("runtime = %s" % (datetime.now() - start_time))

    return json_plot_data
    

def main(site_code='', end='', Print=True, write_to_db=True,
         print_plot=False, save_plot=True, days='', is_command_line_run=True, output_path=''):
    """Computes alert and plots rainfall data.
    
    Args:
        site_code (list): Site codes to compute rainfall analysis for. Optional.
                          Defaults to empty string which will compute alert
                          and plot for all sites.
        Print (bool): To print plot and summary of alerts. Optional. Defaults to
                      True.
        end (datetime): Timestamp of alert and plot to be computed. Optional.
                        Defaults to current timestamp.

    Returns:
        str: Json format of cumulative rainfall and alert per site.
    
    """

    start_time = datetime.now()
    qdb.print_out(start_time)

    if is_command_line_run and len(sys.argv) > 1:
        site_code = sys.argv[1].lower()
            
    if end == '':
        try:
            end = pd.to_datetime(sys.argv[2])
        except:
            end = datetime.now()
    else:
        end = pd.to_datetime(end)
    sc = mem.server_config()
    
    
    if not output_path:
        output_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                                       '../../..'))
        
        
    
        #creates directory if it doesn't exist
        if (sc['rainfall']['print_plot'] or sc['rainfall']['print_summary_alert']) and Print:
            if not os.path.exists(output_path+sc['fileio']['rainfall_path']):
                os.makedirs(output_path+sc['fileio']['rainfall_path'])

    # setting monitoring window
    if days != '':
        sc['rainfall']['rt_window_length'] = days
    end, start, offsetstart = get_rt_window(float(sc['rainfall']['rt_window_length']),
                            float(sc['rainfall']['roll_window_length']), end=end)
    tsn=end.strftime("%Y-%m-%d_%H-%M-%S")

    # 4 nearest rain gauges of each site with threshold and distance from site
    gauges = rainfall_gauges()
    if site_code != '':
        site_code = site_code.replace(' ', '').split(',')												 
        gauges = gauges[gauges.site_code.isin(site_code)]
    gauges['site_id'] = gauges['site_id'].apply(lambda x: float(x))

    trigger_symbol = mem.get('df_trigger_symbols')
    trigger_symbol = trigger_symbol[trigger_symbol.trigger_source == 'rainfall']
    trigger_symbol['trigger_sym_id'] = trigger_symbol['trigger_sym_id'].apply(lambda x: float(x))
    site_props = gauges.groupby('site_id', as_index=False)
    
    summary = site_props.apply(ra.main, end=end, sc=sc,
                                trigger_symbol=trigger_symbol, write_to_db=write_to_db)
    summary = summary.reset_index(drop=True)[['site_id', 'site_code',
                    '1D cml', 'half of 2yr max', '3D cml', '2yr max',
                    'DataSource', 'alert']]
                    
    if Print == True:
        if sc['rainfall']['print_summary_alert']:
            summary.to_csv(output_path+sc['fileio']['rainfall_path'] +
                        'SummaryOfRainfallAlertGenerationFor'+tsn+'.csv',
                        sep=',', mode='w', index=False)
        if sc['rainfall']['print_plot'] or print_plot:
            rain_data = site_props.apply(rp.main, offsetstart=offsetstart,
                                         tsn=tsn, save_plot=save_plot, sc=sc,
                                         start=start, output_path=output_path,
                                         end=end).reset_index(drop=True)
            summary = pd.merge(summary, rain_data, on='site_id',
                               validate='1:1')
    
    summary_json = summary.to_json(orient="records")
    
    qdb.print_out("runtime = %s" %(datetime.now()-start_time))
    
    return summary_json

###############################################################################

if __name__ == "__main__":
    main()
