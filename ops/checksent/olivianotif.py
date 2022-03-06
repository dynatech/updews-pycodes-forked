from datetime import datetime, timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.lib.lib as lib
import ops.lib.querydb as qdb
import ops.lib.sms as sms
import ops.lib.bulletin as bulletin
import ops.lib.raininfo as raininfo


def get_notif(time_now, start, sched, notif_type):
    if (time_now - start).total_seconds()/60 < 5:
        name = 'queued'
        col_name = 'ts_written'
    else:
        name = 'sent'
        col_name = 'ts_sent'
    
    if len(sched) != 0:
            unqueued_unsent = sched.loc[sched[col_name].isnull(), ['site_code', 'fullname']].drop_duplicates()
            msg = 'un{name} {notif_type} ({ts})'.format(name=name, notif_type=notif_type, ts=start)
            if len(unqueued_unsent) != 0:
                msg += ':\n'
                msg += '\n'.join(sorted(unqueued_unsent.apply(lambda row: (row.site_code).upper() + ' ' + row.fullname, axis=1).values))
            else:
                msg = 'No ' + msg
            msg = msg[0].upper() + msg[1:]
    else:
        msg = 'No scheduled {notif_type} ({ts})'.format(notif_type=notif_type, ts=start)
        
    return msg

def main(time_now=datetime.now(), mysql=True, to_csv=False):
    sent_end = lib.release_time(time_now)
    start = sent_end - timedelta(hours=4)
    sent_start = start - timedelta(hours=0.5)
    end = start
    
    site_names = lib.get_site_names()
    sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)
    
    if len(sched) != 0:
        sms_recipients = qdb.get_sms_recipients(mysql=mysql, to_csv=to_csv)
        sms_sent = qdb.get_sms_sent(sent_start, sent_end, site_names, mysql=mysql, to_csv=to_csv)
        sms_sched = sms.ewi_sched(sched, sms_recipients, sms_sent, site_names)
    
        bulletin_recipients = qdb.get_bulletin_recipients(mysql=mysql, to_csv=to_csv)
        bulletin_sent = qdb.get_bulletin_sent(sent_start, sent_end, mysql=mysql, to_csv=to_csv)
        bulletin_sched = bulletin.ewi_sched(sched, bulletin_recipients, bulletin_sent)
    
        rain_recipients = qdb.get_rain_recipients(mysql=mysql, to_csv=to_csv)
        rain_sent = qdb.get_rain_sent(sent_start, sent_end, mysql=mysql, to_csv=to_csv)
        rain_sched = raininfo.ewi_sched(sched, rain_recipients, rain_sent, site_names)
    
    else:
        sms_sched = pd.DataFrame()
        bulletin_sched = pd.DataFrame()
        rain_sched = pd.DataFrame()
        
    sms_notif = get_notif(time_now, start, sms_sched, notif_type='EWI SMS')
    bulletin_notif = get_notif(time_now, start, bulletin_sched, notif_type='EWI bulletin')
    rain_notif = get_notif(time_now, start, rain_sched, notif_type='rain info')
    notif = '\n'.join([sms_notif, bulletin_notif, rain_notif])
    return notif