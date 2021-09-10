from datetime import datetime
import os
import pandas as pd
import sys

path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path)
import alertlib as lib
import proc
import rtwindow as rtw
import trendingalert as trend
sys.path.remove(path)
import analysis.querydb as qdb
#------------------------------------------------------------------------------


def main(tsm_name='', end='', end_mon=False):
    run_start = datetime.now()
    qdb.print_out(run_start)
    qdb.print_out(tsm_name)

    if tsm_name == '':
        tsm_name = sys.argv[1].lower()

    if end == '':
        try:
            end = pd.to_datetime(sys.argv[2])
        except:
            end = datetime.now()
    else:
        end = pd.to_datetime(end)
    
    window, sc = rtw.get_window(end)

    tsm_props = qdb.get_tsm_list(tsm_name)[0]
    data = proc.proc_data(tsm_props, window, sc)
    tilt = data.tilt[window.start:window.end]
    lgd = data.lgd
    tilt = tilt.reset_index().sort_values('ts',ascending=True)
    
    if lgd.empty:
        qdb.print_out('%s: no data' %tsm_name)
        return

    nodal_tilt = tilt.groupby('node_id', as_index=False)     
    alert = nodal_tilt.apply(lib.node_alert, colname=tsm_props.tsm_name, num_nodes=tsm_props.nos, disp=float(sc['subsurface']['disp']), vel2=float(sc['subsurface']['vel2']), vel3=float(sc['subsurface']['vel3']), k_ac_ax=float(sc['subsurface']['k_ac_ax']), lastgooddata=lgd, window=window, sc=sc).reset_index(drop=True)
    
    alert.loc[:, 'col_alert'] = -1
    col_alert = pd.DataFrame({'node_id': range(1, tsm_props.nos+1), 'col_alert': [-1]*tsm_props.nos})
    node_col_alert = col_alert.groupby('node_id', as_index=False)
    node_col_alert.apply(lib.column_alert, alert=alert, num_nodes_to_check=int(sc['subsurface']['num_nodes_to_check']), k_ac_ax=float(sc['subsurface']['k_ac_ax']), vel2=float(sc['subsurface']['vel2']), vel3=float(sc['subsurface']['vel3']))
    
    valid_nodes_alert = alert.loc[~alert.node_id.isin(data.inv)]
    
    if max(valid_nodes_alert['col_alert'].values) > 0:
        pos_alert = valid_nodes_alert[valid_nodes_alert.col_alert > 0]
        site_alert = trend.main(pos_alert, tsm_props.tsm_id, window.end, data.inv)
    else:
        site_alert = max(lib.get_mode(list(valid_nodes_alert['col_alert'].values)))
        
    tsm_alert = pd.DataFrame({'ts': [window.end], 'tsm_id': [tsm_props.tsm_id], 'alert_level': [site_alert], 'ts_updated': [window.end]})

    qdb.alert_to_db(tsm_alert, 'tsm_alerts')
    
    qdb.write_op_trig(tsm_props.site_id, window.end)

    qdb.print_out(tsm_alert)
    
    qdb.print_out('run time = ' + str(datetime.now()-run_start))
    
    return tilt

################################################################################

if __name__ == "__main__":
    
    main()
    
#    # test
#    tilt = main('magta', '2016-10-12 12:00')
