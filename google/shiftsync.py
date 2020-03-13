from datetime import datetime, timedelta
import pandas as pd

import dynadb.db as db
import gsm.smsparser2.smsclass as sms


def get_sheet(key, sheet_name):
    url = 'https://docs.google.com/spreadsheets/d/{key}/gviz/tq?tqx=out:csv&sheet={sheet_name}&headers=1'.format(
        key=key, sheet_name=sheet_name.replace(' ', '%20'))
    df = pd.read_csv(url)
    df = df.drop([col for col in df.columns if col.startswith('Unnamed')], axis=1)
    return df

def get_shift(key, sheet_name):
    df = get_sheet(key, sheet_name)
    df = df.drop([col for col in df.columns if col.startswith('Unnamed')], axis=1)
    df.loc[:, 'Date'] = pd.to_datetime(df.loc[:, 'Date'].ffill())
    df.loc[:, 'Shift'] = df.loc[:, 'Shift'].map({'AM': 8, 'PM': 20})
    df.loc[:, 'ts'] = df.loc[:, ['Date', 'Shift']].apply(lambda row: row.Date + timedelta(hours=row.Shift), axis=1)
    df = df.rename(columns={'IOMP-MT': 'iompmt', 'IOMP-CT': 'iompct'})
    return df.loc[:, ['ts', 'iompmt', 'iompct']]

def main(key):
    ts = datetime.now()
    sheet_name = ts.strftime('%B %Y')
    shift_sched = get_shift(key, sheet_name)
    try:
        sheet_name = (ts+timedelta(weeks=2)).strftime('%B %Y')
        shift_sched = shift_sched.append(get_shift(key, sheet_name))
    except:
        print("no shift schedule for next month")
    data_table = sms.DataTable('monshiftsched', shift_sched)
    db.df_write(data_table)

if __name__ == '__main__':
    key = "1UylXLwDv1W1ukT4YNoUGgHCHF-W8e3F8-pIg1E024ho"
    main(key)
