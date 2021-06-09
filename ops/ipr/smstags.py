# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import datetime, date, time
import matplotlib.pyplot as plt
import pandas as pd


import dynadb.db as db


def outbox_tag(start, end, mysql = False):
    if mysql:
        query  = "SELECT * FROM "
        query += "  (SELECT outbox_id, ts_written, ts_sent, sim_num, mobile_id, sms_msg, send_status, user_id, mn.gsm_id FROM "
        query += "    (SELECT * FROM comms_db.smsoutbox_users "
        query += "    WHERE ts_written BETWEEN '{start}' AND '{end}' "
        query += "    ) AS sms "
        query += "  INNER JOIN "
        query += "    (SELECT * FROM comms_db.smsoutbox_user_status "
        query += "    WHERE outbox_id >= ( "
        query += "      SELECT min(outbox_id) FROM comms_db.smsoutbox_users "
        query += "      WHERE ts_written BETWEEN '{start}' AND '{end}') "
        query += "    ) stat "
        query += "  USING (outbox_id) "
        query += "  INNER JOIN "
        query += "    comms_db.mobile_numbers mn "
        query += "  USING (mobile_id) "
        query += "  INNER JOIN "
        query += "    comms_db.user_mobiles "
        query += "  USING (mobile_id) "
        query += "  ) as msg "
        query += "INNER JOIN "
        query += "  (SELECT user_id, site_code, org_name FROM "
        query += "    commons_db.user_organizations AS org "
        query += "  INNER JOIN "
        query += "    commons_db.sites "
        query += "  USING (site_id) "
        query += "  ) AS site_org "
        query += "USING (user_id) "
        query += "INNER JOIN "
        query += "  (SELECT outbox_id, tag FROM "
        query += "    (SELECT * FROM comms_db.smsoutbox_user_tags "
        query += "    WHERE outbox_id >= ( "
        query += "      SELECT min(outbox_id) FROM comms_db.smsoutbox_users "
        query += "      WHERE ts_written BETWEEN '{start}' AND '{end}') "
        query += "    ) user_tag "
        query += "  INNER JOIN "
        query += "    comms_db.sms_tags "
        query += "  USING (tag_id) "
        query += "  ) as tags "
        query += "USING (outbox_id) "
        query = query.format(start=start, end=end)
        outbox_tag = db.df_read(query, resource='ops')
        outbox_tag.loc[:, 'ts_sms'] = pd.to_datetime(outbox_tag.ts_written)
        outbox_tag.to_csv('input/outbox_tag.csv', index=False)
    else:
        outbox_tag = pd.read_csv('input/outbox_tag.csv', parse_dates=['ts_written'])
    return outbox_tag

def inbox_tag(start, end, mysql = False):
    if mysql:
        query  = "SELECT inbox_id, ts_sms, ts_stored, read_status, site_code, org_name, sim_num, sms_msg, tag FROM "
        query += "  (SELECT inbox_id, ts_sms, ts_stored, sim_num, sms_msg, read_status, user_id FROM "
        query += "    (SELECT * FROM comms_db.smsinbox_users "
        query += "    where ts_sms BETWEEN '{start}' AND '{end}' "
        query += "    ) sms "
        query += "  LEFT JOIN "
        query += "    comms_db.mobile_numbers "
        query += "  USING (mobile_id) "
        query += "  LEFT JOIN "
        query += "    comms_db.user_mobiles "
        query += "  USING (mobile_id) "
        query += "  ) AS msg "
        query += "LEFT JOIN "
        query += "  (SELECT user_id, site_code, org_name FROM "
        query += "    commons_db.user_organizations "
        query += "  INNER JOIN "
        query += "    commons_db.sites "
        query += "  USING (site_id) "
        query += "  ) AS site_org "
        query += "USING (user_id) "
        query += "INNER JOIN "
        query += "  (SELECT inbox_id, tag FROM "
        query += "    comms_db.smsinbox_user_tags "
        query += "  INNER JOIN "
        query += "    comms_db.sms_tags "
        query += "  USING (tag_id) "
        query += "  ) as tags "
        query += "USING (inbox_id) "
        query = query.format(start=start, end=end)
        inbox_tag = db.df_read(query, resource='ops')
        inbox_tag.loc[:, 'ts_sms'] = pd.to_datetime(inbox_tag.ts_sms)
        inbox_tag.to_csv('input/inbox_tag.csv', index=False)
    else:
        inbox_tag = pd.read_csv('input/inbox_tag.csv', parse_dates=['ts_sms'])
    return inbox_tag

