from datetime import timedelta
import numpy as np
import pandas as pd

import dynadb.db as db
import ewisms_meal


def system_downtime(mysql=False):
    if mysql:
        query = 'SELECT * FROM system_down'
        df = db.df_read(query=query, resource="sensor_data")
    else:
        df = pd.read_csv('input/downtime.csv')
    return df

def remove_downtime(sending_status, downtime):
    for start, end in downtime[['start_ts', 'end_ts']].values:
        sending_status = sending_status.loc[(sending_status.ts_start < start) | (sending_status.ts_start > end), :]
    return sending_status 

def main(start, end, recompute=False, mysql=True):
    if recompute:
        ewisms_meal.main(start=start, end=end, mysql=mysql)

    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    ewi_sms = pd.read_csv('output/sending_status.csv')
    ewi_sms.loc[:, ['ts_start', 'ts_end', 'sent']] = ewi_sms.loc[:, ['ts_start', 'ts_end', 'sent']].apply(pd.to_datetime)
    downtime = system_downtime(mysql=mysql)
    ewi_sms = remove_downtime(ewi_sms, downtime)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            sending_status = ewi_sms.loc[(ewi_sms.ts_start > ts) & (ewi_sms.ts_start <= ts+timedelta(0.5)), :]
            if len(sending_status) == 0:
                indiv_ipr.loc[indiv_ipr.Output2.str.contains('EWI SMS', na=False), str(ts)] = np.nan
            elif sum(sending_status.tot_unsent) == 0:
                # all sent on time
                indiv_ipr.loc[indiv_ipr.Output2.str.contains('EWI SMS', na=False), str(ts)] = 1
            else:
                sending_status.loc[:, 'deduction'] = 0.1 * np.ceil((sending_status.sent - sending_status.ts_end).dt.total_seconds()/600)
                sending_status.loc[sending_status.deduction > 1, 'deduction'] = 1
                sending_status.loc[sending_status.deduction < 0, 'deduction'] = 0
                sending_status.loc[:, 'deduction'].fillna(1, inplace = True)
                indiv_ipr.loc[indiv_ipr.Output2.str.contains('EWI SMS', na=False), str(ts)] = np.round((sum(sending_status.min_recipient) - sum(sending_status.tot_unsent * sending_status.deduction)) / sum(sending_status.min_recipient), 2)
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
