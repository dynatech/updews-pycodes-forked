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
    return dyna, sandbox




def ProcAlerts_ref(alert_ref, internal_ref, public_ref):
    TS = alert_ref['ts'].values[0]
    pub_ref = public_ref[(public_ref.ts <= TS)&(public_ref.ts_updated >= TS)]
    pub_sym_ref = pub_ref['alert_level'].values[0]
    alert_type = pub_ref['alert_type'].values[0]
    int_sym_ref = internal_ref[(internal_ref.ts <= TS)&(internal_ref.ts_updated >= TS)]['internal_sym'].values[0]
    if alert_type == 'event' and pub_sym_ref != -1:
        alert_sym = 'A' + str(pub_sym_ref) + '-' + int_sym_ref
    elif alert_type == 'event' and pub_sym_ref == -1:
        alert_sym = 'ND' + '-' + int_sym_ref
    elif alert_type == 'routine' and pub_sym_ref == -1:
        alert_sym = 'ND'
    else:
        alert_sym = 'A0'
    alert_ref['alert'] = alert_sym
    return alert_ref

def SiteAlerts_ref(site_ref, internal_ref, public_ref, ts):
    site = site_ref['site_code'].values[0]
    site_internal_ref = internal_ref[internal_ref.site_code == site]
    site_public_ref = public_ref[public_ref.site_code == site]
    alert_refDF = pd.DataFrame({'site_code': [site]*len(ts), 'ts': ts})
    alert_refTS = alert_refDF.groupby('ts', as_index=False)
    alert_ref = alert_refTS.apply(ProcAlerts_ref, internal_ref=site_internal_ref, public_ref=site_public_ref)
    return alert_ref

def ProcAlerts(alert, internal):
    TS = alert['ts'].values[0]
    alert_sym = internal[(internal.timestamp <= TS)&(internal.updateTS >= TS)]['alert'].values[0]
    alert['alert'] = alert_sym
    return alert

def SiteAlerts(site, internal, ts):
    site = site['name'].values[0]
    site_internal = internal[internal.site == site]
    alertDF = pd.DataFrame({'site_code': [site]*len(ts), 'ts': ts})
    alertTS = alertDF.groupby('ts', as_index=False)
    alert = alertTS.apply(ProcAlerts, internal=site_internal)
    return alert

def to_DB(df):
    if not qdb.does_table_exist('db_comparison', hostdb='sandbox'):
        create_db_comparison()
    df = df.drop('index', axis=1)
    query = "SELECT EXISTS (SELECT * FROM db_comparison"
    query += " WHERE ts = '%s' AND site_code = '%s')" %(df['ts'].values[0], df['site_code'].values[0])
    if qdb.get_db_dataframe(query, hostdb='sandbox').values[0][0] == 0:
        qdb.push_db_dataframe(df, 'db_comparison', index=False)

def main(ts):
    ### INTERNAL ALERTS
    
    query = "SELECT ts, site_code, internal_sym, ts_updated FROM internal_alerts AS i"
    query += " LEFT JOIN sites AS s"
    query += " ON i.site_id = s.site_id"
    query += " ORDER BY ts_updated DESC"
    internal_ref = qdb.get_db_dataframe(query, hostdb='sandbox')
    
    query = "SELECT timestamp, site, alert, updateTS FROM site_level_alert"
    query += " WHERE source = 'internal'"
    query += " ORDER BY updateTS DESC"
    internal = qdb.get_db_dataframe(query, hostdb='dyna')
    
    ### PUBLIC ALERTS
    
    query = "SELECT ts, site_code, alert_level, alert_type, ts_updated FROM"
    query += " (SELECT ts, site_id, alert_level, alert_type, ts_updated FROM public_alerts AS pub"
    query += " LEFT JOIN public_alert_symbols AS sym"
    query += " ON pub.pub_sym_id = sym.pub_sym_id) AS sub"
    query += " LEFT JOIN sites as s"
    query += " ON sub.site_id = s.site_id"
    public_ref = qdb.get_db_dataframe(query, hostdb='sandbox')
        
    ### Sandbox
    
    query = "SELECT site_code FROM sites WHERE site_code != 'mes' ORDER BY site_code"
    site_ref = qdb.get_db_dataframe(query, hostdb='sandbox')
    
    site_ref_grp = site_ref.groupby('site_code', as_index=False)
    alert_ref = site_ref_grp.apply(SiteAlerts_ref, internal_ref=internal_ref, public_ref=public_ref, ts=ts)
    alert_ref = alert_ref.reset_index(drop=True)
    
    ### Dyna Server
    
    query = "SELECT name FROM rain_props WHERE name != 'mes' ORDER BY name"
    site = qdb.get_db_dataframe(query, hostdb='dyna')
    
    site_grp = site.groupby('name', as_index=False)
    alert = site_grp.apply(SiteAlerts, internal=internal, ts=ts)
    alert = alert.reset_index(drop=True)

    diff_alert = alert[~(alert.alert == alert_ref.alert)]
    diff_alert['alert_ref'] = alert_ref[~(alert.alert == alert_ref.alert)]['alert'].values
    grp_diff_alert = diff_alert.reset_index().groupby('index', as_index=False)
    grp_diff_alert.apply(to_DB)

    return alert_ref, alert, diff_alert
    
if __name__ == '__main__':
#    start_time = datetime.now()
#    ### PERIOD OF COMPARISON
#    start = '2017-05-31 16:00'
#    end = '2017-06-01 10:00'
#    ts = pd.date_range(start=start, end=end, freq='1H')
#    alert_ref, alert, diff_alert = main(ts)#[RoundDataTS(datetime.now())])
#    print 'runtime =', datetime.now() - start_time

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))