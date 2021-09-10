from datetime import datetime
import numpy as np
import os
import pandas as pd
import re
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.ipr.ewisms_meal as sms


def ewi_stats(df, quarter=False):
    actual_sent = df.loc[df.ts_end >= df.actual_sent, :]
    actual_sent.loc[:, 'tot_unsent'] = actual_sent.unsent.apply(lambda x: len(re.findall("[\w]{4}", str(x))))
    sent = 100 * (sum(actual_sent.min_recipient) - sum(actual_sent.tot_unsent))/sum(df.min_recipient)
    queued = 100 * (1 - sum(df.tot_unsent) / sum(df.min_recipient))
    ts = pd.to_datetime(df.ts_start.values[0])
    if quarter:
        ts = '{year} Q{quarter}'.format(quarter=int(np.ceil(ts.month/3)), year=ts.year)
    else:
        ts = ts.strftime('%b %Y')
    temp = pd.DataFrame({'ts': [ts], 'sent': [sent], 'queued': [queued]})
    return temp


def main(start, end, mysql=True, write_csv=False):
    all_ewi_sms = sms.main(start=start, end=end, mysql=mysql, write_csv=write_csv)
    all_ewi_sms.loc[:, ['ts_start', 'ts_end', 'queued', 'actual_sent']] = all_ewi_sms.loc[:, ['ts_start', 'ts_end', 'queued', 'actual_sent']].apply(pd.to_datetime)

    all_ewi_sms.loc[:, 'month'] = all_ewi_sms.ts_start.dt.month
    stat = all_ewi_sms.groupby('month', as_index=False).apply(ewi_stats).reset_index(drop=True)
    all_ewi_sms.loc[:, 'quarter'] = np.ceil(all_ewi_sms.month/3)
    stat = stat.append(all_ewi_sms.groupby('quarter', as_index=False).apply(ewi_stats, quarter=True).reset_index(drop=True), ignore_index=True)
    
    return stat

###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2021-01-01')
    end = pd.to_datetime('2021-04-01')
#    stat = main(start, end)
    
    mysql=True
    write_csv=False
    all_ewi_sms = sms.main(start=start, end=end, mysql=mysql, write_csv=write_csv)
    all_ewi_sms.loc[:, ['ts_start', 'ts_end', 'queued', 'actual_sent']] = all_ewi_sms.loc[:, ['ts_start', 'ts_end', 'queued', 'actual_sent']].apply(pd.to_datetime)

    all_ewi_sms.loc[:, 'month'] = all_ewi_sms.ts_start.dt.month
    stat = all_ewi_sms.groupby('month', as_index=False).apply(ewi_stats).reset_index(drop=True)
    all_ewi_sms.loc[:, 'quarter'] = np.ceil(all_ewi_sms.month/3)
    stat = stat.append(all_ewi_sms.groupby('quarter', as_index=False).apply(ewi_stats, quarter=True).reset_index(drop=True), ignore_index=True)

        
    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))