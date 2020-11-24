from datetime import datetime, timedelta, time
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import querydb as qdb

def release_time(date_time):
    """Rounds time to 4/8/12 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off. 04:00 to 07:30 is
        rounded off to 8:00, 08:00 to 11:30 to 12:00, etc.

    Returns:
        datetime: Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    time_hour = int(date_time.strftime('%H'))

    quotient = int(time_hour / 4)

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time

def round_data_ts(date_time):
    """Rounds time to HH:00 or HH:30.

    Args:
        date_time (datetime): Timestamp to be rounded off. Rounds to HH:00
        if before HH:30, else rounds to HH:30.

    Returns:
        datetime: Timestamp with time rounded off to HH:00 or HH:30.

    """

    hour = date_time.hour
    minute = date_time.minute

    if minute < 30:
        minute = 0
    else:
        minute = 30

    date_time = datetime.combine(date_time.date(), time(hour, minute))
    
    return date_time

def event_start(site_id, end):
    """Timestamp of start of event monitoring. Start of event is computed
    by checking if event progresses from non A0 to higher alert.

    Args:
        site_id (int): ID of each site.
        end (datetime): Current public alert timestamp.

    Returns:
        datetime: Timestamp of start of monitoring.
    """

    query =  "SELECT ts, ts_updated FROM "
    query += "  (SELECT * FROM public_alerts "
    query += "  WHERE site_id = %s " %site_id
    query += "  AND (ts_updated <= '%s' " %end
    query += "    OR (ts_updated >= '%s' " %end
    query += "      AND ts <= '%s')) " %end
    query += "  ) AS pub " %site_id
    query += "INNER JOIN "
    query += "  (SELECT * FROM public_alert_symbols "
    query += "  WHERE alert_type = 'event') AS sym "
    query += "ON pub.pub_sym_id = sym.pub_sym_id "
    query += "ORDER BY ts DESC LIMIT 3"
    
    # previous positive alert
    prev_pub_alerts = qdb.get_db_dataframe(query)

    if len(prev_pub_alerts) == 1:
        start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[0])
    # two previous positive alert
    elif len(prev_pub_alerts) == 2:
        # one event with two previous positive alert
        if pd.to_datetime(prev_pub_alerts['ts'].values[0]) - \
                pd.to_datetime(prev_pub_alerts['ts_updated'].values[1]) <= \
                timedelta(hours=0.5):
            start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[1])
        else:
            start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[0])
    # three previous positive alert
    else:
        if pd.to_datetime(prev_pub_alerts['ts'].values[0]) - \
                pd.to_datetime(prev_pub_alerts['ts_updated'].values[1]) <= \
                timedelta(hours=0.5):
            # one event with three previous positive alert
            if pd.to_datetime(prev_pub_alerts['ts'].values[1]) - \
                    pd.to_datetime(prev_pub_alerts['ts_updated'].values[2]) \
                    <= timedelta(hours=0.5):
                start_monitor = pd.to_datetime(prev_pub_alerts['timestamp']\
                        .values[2])
            # one event with two previous positive alert
            else:
                start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[1])
        else:
            start_monitor = pd.to_datetime(prev_pub_alerts['ts'].values[0])

    return start_monitor

def get_operational_trigger(site_id, start_monitor, end):
    """Dataframe containing alert level on each operational trigger
    from start of monitoring.

    Args:
        site_id (dataframe): ID each site.
        start_monitor (datetime): Timestamp of start of monitoring.
        end (datetime): Public alert timestamp.

    Returns:
        dataframe: Contains timestamp range of alert, three-letter site code,
                   operational trigger, alert level, and alert symbol from
                   start of monitoring
    """

    query =  "SELECT op.trigger_id, op.trigger_sym_id, ts, site_id, source_id, alert_level, "
    query += "alert_symbol, ts_updated FROM"
    query += "  (SELECT * FROM operational_triggers "
    query += "  WHERE site_id = %s" %site_id
    query += "  AND ts_updated >= '%s' AND ts <= '%s' "%(start_monitor, end)
    query += "  ) AS op "
    query += "INNER JOIN "
    query += "  operational_trigger_symbols AS sym "
    query += "ON op.trigger_sym_id = sym.trigger_sym_id "
    query += "ORDER BY ts DESC"

    op_trigger = qdb.get_db_dataframe(query)
    
    return op_trigger