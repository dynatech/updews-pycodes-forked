from datetime import datetime, date, time
import os
import pandas as pd

import querydb as qdb

def create_db_comparison():
    query = "CREATE TABLE `db_comparison` ("
    query += "  `comparison_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NULL,"
    query += "  `site_code` CHAR(3) NOT NULL,"
    query += "  `alert` VARCHAR(12) NOT NULL,"
    query += "  `alert_ref` VARCHAR(12) NOT NULL,"
    query += "  PRIMARY KEY (`comparison_id`),"
    query += "  UNIQUE INDEX `uq_db_comparison` (`ts` ASC, `site_code` ASC))"
    
    qdb.execute_query(query, hostdb='sandbox')

def data_ts(endpt):
    year = endpt.year
    month = endpt.month
    day = endpt.day
    hour = endpt.hour
    minute = endpt.minute
    if minute < 30:
        minute = 0
    else:
        minute = 30
    end = datetime.combine(date(year, month, day), time(hour, minute))
    return end

def json_files():
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sc = qdb.memcached()
    dyna = pd.read_json(output_path+sc['fileio']['output_path']+'PublicAlert.json')
    dyna = pd.DataFrame(dyna['alerts'].values[0])
    sandbox = pd.read_json(output_path+sc['fileio']['output_path']+'PublicAlertRefDB.json')
    sandbox = pd.DataFrame(sandbox['alerts'].values[0])
    return dyna, sandbox
    
def to_DB(df):
    df = df.rename(columns = {'dyna': 'alert', 'sandbox': 'alert_ref'})
    if not qdb.does_table_exist('db_comparison', hostdb='sandbox'):
        create_db_comparison()
    query = "SELECT EXISTS (SELECT * FROM db_comparison"
    query += " WHERE ts = '%s' AND site_code = '%s')" %(df['ts'].values[0], df['site_code'].values[0])
    if qdb.get_db_dataframe(query, hostdb='sandbox').values[0][0] == 0:
        qdb.push_db_dataframe(df, 'db_comparison', index=False)

def main():
    dyna, sandbox = json_files()
    dyna = dyna.rename(columns = {'timestamp': 'ts', 'site': 'site_code', \
            'alert': 'public_alert', 'sensor_alert': 'subsurface', \
            'rain_alert': 'rainfall', 'ground_alert': 'surficial', \
            'retriggerTS': 'triggers'})
    if max(dyna['ts'].values) == max(sandbox['ts'].values):
        dyna = dyna.set_index('site_code').sort_index()
        sandbox = sandbox.set_index('site_code').sort_index()
        comparison = pd.DataFrame(index=dyna.index)
        comparison['dyna'] = dyna['internal_alert']
        comparison['sandbox'] = sandbox['internal_alert']

        diff = comparison[~(comparison['dyna'] == comparison['sandbox'])].reset_index()
        diff['ts'] = max(sandbox['ts'].values)
        qdb.print_out(diff)
        site_diff = diff.groupby('site_code', as_index=False)
        site_diff.apply(to_DB)
        
if __name__ == '__main__':
    main()