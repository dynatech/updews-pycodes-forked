from datetime import date, time, datetime, timedelta
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys

plt.ion()

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import analysis.subsurface.plotterlib as plotter
import analysis.subsurface.proc as proc
import analysis.subsurface.rtwindow as rtw


def main():

    # asks for tsm name
    while True:
        props = qdb.get_tsm_list(input('sensor name: '))
        if len(props) == 1:
            break
        else:
            qdb.print_out('sensor name is not in the list')
            continue

    tsm_props = props[0]

    # asks if to plot from date activated (or oldest data) to most recent data
    while True:
        input_text = 'plot from start to end of data? (Y/N): '
        plot_all_data = input(input_text).lower()
        if plot_all_data == 'y' or plot_all_data == 'n':
            break

    # asks if to specify end timestamp of monitoring window
    if plot_all_data == 'n':
        while True:
            input_text = 'specify end timestamp of monitoring window? (Y/N): '
            test_specific_time = input(input_text).lower()
            if test_specific_time == 'y' or test_specific_time == 'n':
                break

        # ask for timestamp of end of monitoring window defaults to datetime.now
        if test_specific_time == 'y':
            while True:
                try:
                    input_text = 'plot end timestamp (format: 2016-12-31 23:30): '
                    end = pd.to_datetime(input(input_text))
                    break
                except:
                    print ('invalid datetime format')
                    continue
        else:
            end = datetime.now()
            
        # monitoring window and server configurations
        window, sc = rtw.get_window(end)
        
        # asks if to plot with 3-day monitoring window
        while True:
            input_text = 'plot with 3-day monitoring window? (Y/N): '
            three_day_window = input(input_text).lower()
            if three_day_window == 'y' or three_day_window == 'n':
                break
        
        # asks start of monitoring window defaults to values in server config
        if three_day_window == 'n':
            while True:
                input_text = 'start of monitoring window (in days) '
                input_text += 'or datetime (format: 2016-12-31 23:30): '
                start = input(input_text)
                try:
                    window.start = window.end - timedelta(int(start))
                    break
                except:
                    try:
                        window.start = pd.to_datetime(start)
                        break
                    except:
                        print ('datetime format or integer only')
                        continue
                    
            # computes offsetstart from given start timestamp
            window.offsetstart = window.start - timedelta(days=(sc['subsurface']
            ['num_roll_window_ops']*window.numpts-1)/48.)
            
    else:
        # check date of activation
        query = "SELECT date_activated FROM tsm_sensors"
        query += " WHERE tsm_name = '%s'" %tsm_props.tsm_name
        try:
            date_activated = qdb.get_db_dataframe(query).values[0][0]
        except:
            date_activated = pd.to_datetime('2010-01-01')
        #compute for start to end timestamp of data
        query = "(SELECT * FROM tilt_%s" %tsm_props.tsm_name
        query += " where ts > '%s' ORDER BY ts LIMIT 1)" %date_activated
        query += " UNION ALL"
        query += " (SELECT * FROM tilt_%s" %tsm_props.tsm_name
        query += " ORDER BY ts DESC LIMIT 1)"
        start_end = qdb.get_db_dataframe(query)
        
        end = pd.to_datetime(start_end['ts'].values[1])
        window, sc = rtw.get_window(end)
        
        start_dataTS = pd.to_datetime(start_end['ts'].values[0])
        start_dataTS_Year=start_dataTS.year
        start_dataTS_month=start_dataTS.month
        start_dataTS_day=start_dataTS.day
        start_dataTS_hour=start_dataTS.hour
        start_dataTS_minute=start_dataTS.minute
        if start_dataTS_minute<30:start_dataTS_minute=0
        else:start_dataTS_minute=30
        window.offsetstart=datetime.combine(date(start_dataTS_Year,
                            start_dataTS_month, start_dataTS_day),
                            time(start_dataTS_hour, start_dataTS_minute, 0))

        # computes offsetstart from given start timestamp
        window.start = window.offsetstart + timedelta(days=(sc['subsurface']
                                ['num_roll_window_ops']*window.numpts-1)/48.)


    # asks if to plot velocity and asks for interval and legends to show in
    # column position plots if to plot all data or if monitoring window not
    # equal to 3 days    
    if plot_all_data == 'y' or three_day_window == 'n':
        # asks for interval between column position plots
        while True:
            try:
                input_text = 'interval between column position plots, in days: '
                col_pos_interval = int(input(input_text))
                break
            except:
                qdb.print_out('enter an integer')
                continue
            
        # computes for interval and number of column position plots
        sc['subsurface']['col_pos_interval'] = str(col_pos_interval) + 'D'
        sc['subsurface']['num_col_pos'] = int((window.end - window.start).
                                                    days/col_pos_interval + 1)
                                                    
        # asks if to plot all legends
        while True:
            input_text = 'show all legends in column position plot? (Y/N): '
            show_all_legend = input(input_text).lower()
            if show_all_legend == 'y' or show_all_legend == 'n':        
                break

        if show_all_legend == 'y':
            show_part_legend = False
        # asks which legends to show
        elif show_all_legend == 'n':
            while True:
                try:
                    show_part_legend = int(input('every nth legend to show: '))
                    if show_part_legend <= sc['subsurface']['num_col_pos']:
                        break
                    else:
                        input_text = 'integer should be less than '
                        input_text += 'the number of colpos dates to plot: '
                        input_text += '%s' %(sc['subsurface']['num_col_pos'])
                        qdb.print_out(input_text)
                        continue
                except:
                    qdb.print_out('enter an integer')
                    continue
                
        while True:
            plotvel = input('plot velocity? (Y/N): ').lower()
            if plotvel == 'y' or plotvel == 'n':        
                break
            
        if plotvel == 'y':
            plotvel = True
        else:
            plotvel = False
        
        three_day_window = False            
    else:
        plotvel = True
        show_part_legend = True
        three_day_window = True

    # asks which point to fix in column position plots
    while True:
        input_text = 'column fix for colpos (top/bottom); '
        input_text += 'default for monitoring is fix bottom: '
        column_fix = input(input_text).lower()
        if column_fix in ['top', 'bottom', '']:
            break
        
    if column_fix == '':
        column_fix = 'bottom'        
    sc['subsurface']['column_fix'] = column_fix
      
    # mirror xz and/or xy colpos
    while True:
        try:
            mirror_xz = bool(int(input('mirror image of xz colpos? (0/1): ')))
            break
        except:
            print ('Invalid. 1 for mirror image of xz colpos else 0')
            continue
    while True:
        try:
            mirror_xy = bool(int(input('mirror image of xy colpos? (0/1): ')))
            break
        except:
            print ('Invalid. 1 for mirror image of xy colpos else 0')
            continue

    data = proc.proc_data(tsm_props, window, sc, realtime=True, comp_vel=plotvel)
    plotter.main(data, tsm_props, window, sc, plotvel=plotvel,
                 show_part_legend = show_part_legend, realtime=True,
                 plot_inc=False, three_day_window=three_day_window,
                 mirror_xz=mirror_xz, mirror_xy=mirror_xy)

##########################################################
if __name__ == "__main__":
    start = datetime.now()
    main()
    print ('runtime =', str(datetime.now() - start))
