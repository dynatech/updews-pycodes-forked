# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import datetime
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import volatile.memory as mem


def main(start, end):
    conn = mem.get('DICT_DB_CONNECTIONS')
    query  = "SELECT ts_sms, site_code FROM "
    query += "  (SELECT inbox_id, ts_sms, user_id FROM "
    query += "    (SELECT * FROM {gsm_pi}.smsinbox_users "
    query += "    WHERE ts_sms BETWEEN '{start}' AND '{end}' "
    query += "    ) sms "
    query += "  INNER JOIN "
    query += "    {gsm_pi}.user_mobiles "
    query += "  USING (mobile_id) "
    query += "  INNER JOIN "
    query += "    {common}.users "
    query += "  USING (user_id) "
    query += "  ) AS msg "
    query += "INNER JOIN "
    query += "  (SELECT user_id, site_code, org_name FROM "
    query += "    {common}.user_organizations "
    query += "  INNER JOIN "
    query += "    {common}.sites "
    query += "  USING (site_id) "
    query += "  ) AS site_org "
    query += "USING (user_id) "
    query += "INNER JOIN "
    query += "  (SELECT * FROM "
    query += "    {gsm_pi}.smsinbox_user_tags "
    query += "  INNER JOIN "
    query += "    (SELECT tag_id, tag FROM {gsm_pi}.sms_tags "
    query += "    WHERE tag = '#EwiResponse') ack "
    query += "  USING (tag_id) "
    query += "  ) AS tags "
    query += "USING (inbox_id) "
    query += "ORDER BY inbox_id DESC "
    query = query.format(start=start, end=end, common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
    ACK = db.df_read(query, resource='sms_analysis')
    
    ACK.loc[:, 'day'] = ACK.loc[:, 'ts_sms'].map(lambda x: pd.to_datetime(x).isocalendar()[2])
    ACK.loc[:, 'hour'] = ACK.ts_sms.dt.hour
    
    site_hour_ACK = ACK[['ts_sms', 'hour', 'site_code']].groupby(['hour', 'site_code']).count().reset_index()
    hour_ACK = ACK[['ts_sms', 'hour']].groupby(['hour']).count().reset_index()
    site_day_ACK = ACK[['ts_sms', 'day', 'site_code']].groupby(['day', 'site_code']).count().reset_index()
    site_day_ACK = site_day_ACK.sort_values('day')
    site_day_ACK.loc[:, 'day'] = site_day_ACK.day.map({1: 'M', 2: 'T', 3: 'W', 4: 'Th', 5: 'F', 6: 'Sa', 7: 'Su'})
    day_ACK = ACK[['ts_sms', 'day']].groupby(['day']).count().reset_index()
    day_ACK = day_ACK.sort_values('day')
    day_ACK.loc[:, 'day'] = day_ACK.day.map({1: 'M', 2: 'T', 3: 'W', 4: 'Th', 5: 'F', 6: 'Sa', 7: 'Su'})

    return site_hour_ACK, hour_ACK, site_day_ACK, day_ACK


###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2020-07-01')
    end = pd.to_datetime('2020-10-01')
    site_hour_ACK, hour_ACK, site_day_ACK, day_ACK = main(start, end)

    fig = plt.figure()
    ax = fig.add_subplot()
    ax.bar(day_ACK.day, day_ACK.ts_sms)

    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))