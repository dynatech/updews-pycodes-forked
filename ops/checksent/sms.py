from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.lib.lib as lib
import ops.lib.sms as lib_sms


def main(time_now = datetime.now(), mysql=True, to_csv=False):
    end = lib.release_time(time_now) - timedelta(hours=4)
    start = end
    if (time_now - start).total_seconds()/60 < 5:        
        name = 'queued'
        col_name = 'ts_written'
    else:
        name = 'sent'
        col_name = 'ts_sent'
    sent_sched = lib_sms.ewi_sched(start, end, mysql=mysql, to_csv=to_csv)
    
    if len(sent_sched) != 0:
            unqueued_unsent = sent_sched.loc[sent_sched[col_name].isnull(), ['site_code', 'fullname']].drop_duplicates()
            msg = 'un{name} EWI SMS ({ts})'.format(name=name, ts=start)
            if len(unqueued_unsent) != 0:
                msg += ':\n'
                msg += '\n'.join(sorted(unqueued_unsent.apply(lambda row: (row.site_code).upper() + ' ' + row.fullname, axis=1).values))
            else:
                msg = 'No ' + msg
            msg = msg[0].upper() + msg[1:]
    else:
        msg = 'No scheduled EWI SMS ({ts})'.format(ts=start)
        
    return msg