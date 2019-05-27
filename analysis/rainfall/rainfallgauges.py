from datetime import datetime
import numpy as np
import os
import pandas as pd
import requests
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import volatile.memory as mem


def noah_gauges():
    """Gathers information on rain gauges from NOAN or ASTI

    Returns:
        dataframe: available rain gauges from NOAH or ASTI

    """

    sc = mem.server_config()
    r = requests.get(sc['rainfall']['noah_gauges'],
                     auth=(sc['rainfall']['noah_user'],
                           sc['rainfall']['noah_password']))    
    noah = pd.DataFrame(r.json())
    noah = noah[noah['sensor_name'].str.contains('rain', case = False)]
    noah = noah.dropna()
    noah['dev_id'] = noah['dev_id'].apply(lambda x: int(x))
    noah['longitude'] = noah['longitude'].apply(lambda x: np.round(float(x),6))
    noah['latitude'] = noah['latitude'].apply(lambda x: np.round(float(x),6))
    noah = noah.loc[(noah.longitude != 0) & (noah.latitude != 0) & \
                    (noah.date_installed >= str(pd.to_datetime(0)))]
    noah = noah.rename(columns = {'dev_id': 'gauge_name',
                                  'date_installed': 'date_activated'})
    noah['data_source'] = 'noah'
    noah['date_deactivated'] = np.nan
    noah = noah[['gauge_name', 'data_source', 'longitude', 'latitude',
                 'date_activated', 'date_deactivated']]
    return noah

def main():
    """Writes in rainfall_gauges information on available rain gauges 
     for rainfall alert analysis

    """

    start = datetime.now()
    qdb.print_out(start)

    if qdb.does_table_exist('rainfall_gauges') == False:
        #Create a rainfall_gauges table if it doesn't exist yet
        qdb.create_rainfall_gauges()

    senslope = mem.get('df_dyna_rain_gauges')
    senslope = senslope[senslope.has_rain == 1]
    senslope['data_source'] = 'senslope'
    
    noah = noah_gauges()
    
    all_gauges = senslope.append(noah, sort=False)
    all_gauges['gauge_name'] = all_gauges['gauge_name'].apply(lambda x: str(x))
    all_gauges['date_activated'] = pd.to_datetime(all_gauges['date_activated'])
    written_gauges = mem.get('df_rain_gauges')
    not_written = set(all_gauges['gauge_name']) \
                     - set(written_gauges['gauge_name'])
    
    new_gauges = all_gauges[all_gauges.gauge_name.isin(not_written)]
    new_gauges = new_gauges[new_gauges.date_deactivated.isnull()]
    new_gauges = new_gauges[['gauge_name', 'data_source', 'longitude',
                             'latitude', 'date_activated']]
    if len(new_gauges) != 0:
        data_table = sms.DataTable('rainfall_gauges', new_gauges)
        db.df_write(data_table)
    
    deactivated = written_gauges[~written_gauges.date_deactivated.isnull()]
    deactivated = deactivated.gauge_name
    
    deactivated_gauges = all_gauges[(~all_gauges.date_deactivated.isnull()) \
                                  & (~all_gauges.gauge_name.isin(not_written))\
                                  & (~all_gauges.gauge_name.isin(deactivated))]
    date_deactivated = pd.to_datetime(deactivated_gauges['date_deactivated'])
    deactivated_gauges['date_deactivated'] = date_deactivated
    deactivated_gauges = deactivated_gauges[['gauge_name', 'data_source',
                                             'longitude','latitude',
                                             'date_activated']]
    if len(deactivated_gauges) != 0:
        data_table = sms.DataTable('rainfall_gauges', deactivated_gauges)
        db.df_write(data_table)

    qdb.print_out('runtime = %s' %(datetime.now() - start))

################################################################################
if __name__ == "__main__":
    main()
