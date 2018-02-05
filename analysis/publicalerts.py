from datetime import datetime, timedelta, time
import numpy as np
import os
import pandas as pd

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

    quotient = time_hour / 4

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time

def data_ts(date_time):
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

def get_public_symbols():
    """Dataframe containing public alert level and its corresponding symbol.
    """

    query = "SELECT * FROM public_alert_symbols"

    public_symbols = qdb.get_db_dataframe(query)
    public_symbols = public_symbols.sort_values(['alert_type', 'alert_level'],
                                                ascending=[True, False])

    return public_symbols

def get_internal_symbols():
    """Dataframe containing trigger alert level, source and, 
    hierarchy in writing its symbol in internal alert
    """

    query =  "SELECT trigger_sym_id, trig.source_id, trigger_source, "
    query += "alert_level, alert_symbol, hierarchy_id FROM ( "
    query += "  SELECT op.trigger_sym_id, source_id, alert_level, "
    query += "  inte.alert_symbol FROM "
    query += "    internal_alert_symbols AS inte "
    query += "  INNER JOIN "
    query += "	 operational_trigger_symbols AS op "
    query += "  ON op.trigger_sym_id = inte.trigger_sym_id "
    query += "  ) AS sub "
    query += "INNER JOIN "
    query += "  trigger_hierarchies AS trig "
    query += "ON trig.source_id = sub.source_id "
    query += "ORDER BY hierarchy_id"
    
    internal_symbols = qdb.get_db_dataframe(query)
    
    return internal_symbols

def get_trigger_symbols():
    """Dataframe containing operational trigger alert level and 
    its corresponding id/symbol.
    """

    query =  "SELECT trigger_sym_id, alert_level, alert_symbol, "
    query += "op.source_id, trigger_source FROM "
    query += "  operational_trigger_symbols AS op "
    query += "INNER JOIN "
    query += "  trigger_hierarchies AS trig "
    query += "ON op.source_id = trig.source_id"
    
    trig_symbols = qdb.get_db_dataframe(query)
    
    return trig_symbols

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
    query += "  (SELECT * FROM public_alerts"
    query += "  WHERE site_id = %s " %site_id
    query += "  AND (ts_updated <= '%s' " %end
    query += "    OR (ts_updated >= '%s' " %end
    query += "      AND ts <= '%s')) " %end
    query += "  ) AS pub" %site_id
    query += "INNER JOIN"
    query += "  (SELECT * FROM public_alert_symbols"
    query += "  WHERE alert_type = 'event') AS sym"
    query += "ON pub.pub_sym_id = sym.pub_sym_id"
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

def get_monitoring_type(site_id, end):
    """Type of monitoring: 'event' or 'routine'. Extended monitoring is tagged
    as 'routine' since it requires surficial data once a day. For simplicity,
    sites not under monitoring (event, extended, routine) are tagged 'routine'.

    Args:
        site_id (int): ID of each site.
        end (datetime): Current public alert timestamp.

    Returns:
        str: 'event' or 'routine'.
    """

    query =  "SELECT alert_type FROM "
    query += "  (SELECT * FROM public_alerts "
    query += "  WHERE site_id = %s " %site_id
    query += "  AND (ts_updated <= '%s' " %end
    query += "    OR (ts_updated >= '%s' " %end
    query += "      AND ts <= '%s')) " %end
    query += "  ORDER BY ts DESC LIMIT 1 "
    query += "  ) AS pub "
    query += "INNER JOIN "
    query += "  public_alert_symbols AS sym "
    query += "ON pub.pub_sym_id = sym.pub_sym_id"
            
    monitoring_type = qdb.get_db_dataframe(query)['alert_type'].values[0]
    
    return monitoring_type

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

    query =  "SELECT op.trigger_sym_id, ts, site_id, source_id, alert_level, "
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

def nd_internal_alert(no_data, internal_df, internal_symbols):
    """Internal alert sysmbol for event triggers that currently has no data.

    Args:
        no_data (dataframe): Event triggers that currently has no data.
        internal_df (dataframe): Event triggers.
        internal_symbols (dataframe): Internal alert symbols and id
                                      corresponding to its alert level.

    Returns:
        dataframe: event triggers with its corresponding internal alert symbol.
    """

    source_id = no_data['source_id'].values[0]
    alert_level = no_data['alert_level'].values[0]
    max_alert_level = max(internal_symbols[internal_symbols.source_id == \
                                           source_id]['alert_level'].values)
    if alert_level < max_alert_level:
        internal_df.loc[internal_df.source_id == source_id, 'alert_symbol'] = \
                        internal_df[internal_df.source_id == \
                        source_id]['alert_symbol'].values[0].lower()
    return internal_df

def get_internal_alert(pos_trig, release_op_trig, internal_symbols):
    """Current internal alert sysmbol: indicates event triggers and operational
    trigger data presence.

    Args:
        pos_trig (dataframe): Operational triggers with alert level > 0.
        release_op_trigger (dataframe): Operational triggers after previous
                                        release until public alert timestamp.
        internal_symbols (dataframe): Internal alert symbols and id
                                      corresponding to its alert level.

    Returns:
        dataframe: alert symbol indicating event triggers and data presence.
    """

    highest_trigger = pos_trig.sort_values('alert_level',
                        ascending=False).drop_duplicates('source_id')
    with_data = release_op_trig[release_op_trig.alert_level != -1]
    with_data_id = with_data['source_id'].values
    with_data = highest_trigger[highest_trigger.source_id.isin(with_data_id)]
    sym_id = with_data['trigger_sym_id'].values
    no_data = highest_trigger[~highest_trigger.source_id.isin(with_data_id)]
    nd_source_id = no_data['source_id'].values
    internal_df = internal_symbols[(internal_symbols.trigger_sym_id.isin(sym_id)) \
            | ((internal_symbols.source_id.isin(nd_source_id)) & \
               (internal_symbols.alert_level == -1))]
    if len(no_data) != 0:
        no_data_grp = no_data.groupby('source_id', as_index=False)
        internal_df = no_data_grp.apply(nd_internal_alert,
                                        internal_df=internal_df,
                                        internal_symbols=internal_symbols)
    internal_df = internal_df.drop_duplicates()
    internal_df = internal_df.reset_index(drop=True)
    
    return internal_df

def get_tsm_alert(site_id, end):
    """Dataframe containing alert level on each tsm sensor

    Args:
        site_id (dataframe): ID each site.
        end (datetime): Public alert timestamp.

    Returns:
        dataframe: Contains tsm name, alert level, and alert symbol
                   for current release
    """

    query =  "SELECT tsm_name, sub.alert_level FROM "
    query += "  (SELECT tsm_name, alert_level FROM "
    query += "    (SELECT * FROM tsm_alerts "
    query += "     WHERE ts <= '%s' " %end
    query += "    AND ts_updated >= '%s' " %end
    query += "    ORDER BY ts DESC "
    query += "    ) AS alert "
    query += "  INNER JOIN "
    query += "    (SELECT tsm_id, tsm_name FROM tsm_sensors "
    query += "    WHERE site_id = %s " %site_id
    query += "    ) AS tsm "
    query += "  ON tsm.tsm_id = alert.tsm_id) "
    query += "  AS sub "
    query += "INNER JOIN "
    query += "  (SELECT sym.source_id, alert_symbol, alert_level FROM "
    query += "    (SELECT source_id, alert_level, alert_symbol FROM "
    query += "    operational_trigger_symbols "
    query += "    ) AS sym "
    query += "  INNER JOIN "
    query += "    (SELECT source_id FROM trigger_hierarchies "
    query += "    WHERE trigger_source = 'subsurface' "
    query += "    ) AS hier "
    query += "  ON hier.source_id = sym.source_id "
    query += "  ) AS sub2 "
    query += "ON sub.alert_level = sub2.alert_level"

    subsurface = qdb.get_db_dataframe(query)
    subsurface = subsurface.drop_duplicates('tsm_name')
    
    return subsurface

def check_rainfall_alert(internal_df, internal_symbols, site_id,
                         end, rainfall_id, rain75_id):
    """Current internal alert sysmbol: includes rainfall symbol if 
    above 75% of threshold

    Args:
        internal_df (dataframe): Current internal alert level and sysmbol.
        internal_symbols (dataframe): Internal alert symbols and id
                                      corresponding to its alert level.
        site_id (dataframe): ID each site.
        end (datetime): Public alert timestamp.
        rainfall_id (int): id of rainfall operational trigger

    Returns:
        dataframe: alert symbol indicating event triggers, data presence 
                   and status of rainfall.
    """ 
    
    query =  "SELECT * FROM rainfall_alerts "
    query += "where site_id = '%s' " %site_id
    query += "and ts = '%s'" %end
    rainfall_df = qdb.GetDBDataFrame(query)

    if len(rainfall_df) != 0:
        if rainfall_id in internal_df['source_id'].values:
            rain_alert = internal_symbols[internal_symbols.source_id == \
                            rainfall_id]['alert_symbol'].values[0]
            internal_df.loc[internal_df.source_id == rainfall_id,
                            'alert_symbol'] = rain_alert
        else:
            rain_df = internal_symbols[internal_symbols.trigger_sym_id == \
                                        rain75_id]
            rain_df['alert_symbol'] = rain_df['alert_symbol'].apply(lambda x: \
                                                                    x.lower())
            internal_df = internal_df.append(rain_df, ignore_index=True)
                            
    return internal_df

def site_public_alert(site_props, end, public_symbols, internal_symbols,
                      trig_symbols, start_time):  
    """Dataframe containing necessary information for public release.

    Args:
        site_props (dataframe): ID and three-letter code of each site.
        end (datetime): Public alert timestamp.
        public_symbols (dataframe): Public alert symbols and id corresponding
                                    to its alert level.
        internal_symbols (dataframe): Internal alert symbols and id
                                      corresponding to its alert level.
        trig_symbols (dataframe): Operational trigger symbols and id
                                  corresponding to its alert level.

    Returns:
        dataframe: Contains timestamp, three-letter site code, public alert, 
                   internal alert, validity of alert, subsurface alert, 
                   surficial alert, rainfall alert, most recent timestamp of
                   alert > 0 (if any) per alert level per operational trigger.
    """
    
    # id and three-letter code per site
    site_code = site_props['site_code'].values[0]
    site_id = site_props['site_id'].values[0]
    qdb.print_out(site_code)

    # Creates a public_alerts table if it doesn't exist yet
    if qdb.does_table_exist('public_alerts') == False:
        qdb.create_public_alerts()
    
    # start of monitoring: start of event or 24 hours from "end"
    try:
        monitoring_type = get_monitoring_type(site_id, end)
    except:
        monitoring_type = 'routine'
    if monitoring_type == 'event':
        start_monitor = event_start(site_id, end)
    else:
        start_monitor = end - timedelta(1)

    # operational triggers for monitoring at timestamp end
    op_trig = get_operational_trigger(site_id, start_monitor, end) 
    release_op_trig = op_trig[op_trig.ts_updated >= \
            release_time(end)-timedelta(hours=4)]
    release_op_trig = release_op_trig.drop_duplicates(['source_id', \
            'alert_level'])
    subsurface_id = internal_symbols[internal_symbols.trigger_source == \
            'subsurface']['source_id'].values[0]
    release_op_trig = release_op_trig[~((release_op_trig.source_id \
            == subsurface_id) & (release_op_trig.ts_updated < end))]
    pos_trig = op_trig[op_trig.alert_level > 0]
    last_pos_trig = pos_trig.drop_duplicates(['source_id', \
            'alert_level'])

    # public alert based on highest alert level in operational triggers
    public_alert = max(list(op_trig['alert_level'].values) + [0])
    qdb.print_out('Public Alert %s' %public_alert)

    # subsurface alert
    subsurface = get_tsm_alert(site_id, end)

    # surficial alert
    if public_alert > 0:
        surficial_ts = release_time(end) - timedelta(hours=4)
    else:
        surficial_ts = pd.to_datetime(end.date())
    surficial_id = internal_symbols[internal_symbols.trigger_source == \
            'surficial']['source_id'].values[0]
    try:
        surficial = op_trig[(op_trig.source_id == surficial_id) & \
                 (op_trig.ts_updated >= surficial_ts)]['alert_level'].values[0]
    except:
        surficial = -1
            
    # rainfall alert
    rainfall_id = internal_symbols[internal_symbols.trigger_source == \
            'rainfall']['source_id'].values[0]
    try:
        rainfall = op_trig[(op_trig.source_id == rainfall_id) & \
                 (op_trig.ts_updated >= end)]['alert_level'].values[0]
    except:
        rainfall = -1

    # INTERNAL ALERT
    if public_alert > 0:
        # validity of alert
        validity = pd.to_datetime(min(pos_trig['ts_updated'].values)) \
                                 + timedelta(1)
        if public_alert == 3:
            validity += timedelta(1)
        # internal alert based on positive triggers and data presence
        internal_df = get_internal_alert(pos_trig, release_op_trig,       
                                  internal_symbols)
        # check if rainfall > 0.75% of threshold
        rain75_id = internal_symbols[(internal_symbols.source_id == \
                        rainfall_id)&(internal_symbols.alert_level \
                        == -2)]['trigger_sym_id'].values[0]
        if rainfall == 0 and end >= validity - timedelta(hours=0.5):
            internal_df = check_rainfall_alert(internal_df, internal_symbols,
                                               site_id, end, rainfall_id,
                                               rain75_id)
        internal_df = internal_df.sort_values('hierarchy_id')
        internal_alert = ''.join(internal_df['alert_symbol'].values)
        if public_alert > 1:
            internal_alert = public_symbols[public_symbols.alert_level == \
                             public_alert]['alert_symbol'].values[0] + '-' + \
                             internal_alert
    # ground data presence: subsurface, surficial, moms
    if public_alert <= 1:
        if surficial == -1 and len(subsurface[subsurface.alert_level != -1]) == 0:
            ground_alert = -1
        else:
            ground_alert = 0
        internal_id = internal_symbols[internal_symbols.trigger_source == \
                'internal']['source_id'].values[0]
        if public_alert == 0 or ground_alert == -1:
            pub_internal = internal_symbols[(internal_symbols.alert_level == \
                             ground_alert) & (internal_symbols.source_id == \
                             internal_id)]['alert_symbol'].values[0]
            if public_alert == 0:
                internal_alert = ''
                hyphen = ''
            else:
                hyphen = '-'
        else:
            pub_internal = public_symbols[public_symbols.alert_level == \
                             public_alert]['alert_symbol'].values[0] + '-' + \
                             internal_alert
            hyphen = '-'
        internal_alert = pub_internal + hyphen + internal_alert
    elif -1 in internal_df['alert_level'].values:
        ground_alert = -1
    else:
        ground_alert = 0
    
    # PUBLIC ALERT
    # check if end of validity: lower alert if with data and not rain75
    if public_alert > 0:
        # check if end of validity: lower alert if with data and not rain75
        if validity > end + timedelta(hours=0.5):
            pass
        elif (validity + timedelta(3) > end + timedelta(hours=0.5) and \
                        ground_alert == -1) \
                    or rain75_id in internal_df['trigger_sym_id'].values \
                    or not (end.time() in [time(3, 30), time(7, 30),
                        time(11, 30), time(15, 30), time(19, 30),
                        time(23, 30)] and int(start_time.strftime('%M')) > 45):
            validity = release_time(end)
        else:
            validity = '-'
            public_alert = 0
            internal_alert = internal_symbols[(internal_symbols.alert_level == \
                             ground_alert) & (internal_symbols.source_id == \
                             internal_id)]['alert_symbol'].values[0]
    else:
        validity = '-'
        public_alert = 0
        internal_alert = internal_symbols[(internal_symbols.alert_level == \
                         ground_alert) & (internal_symbols.source_id == \
                         internal_id)]['alert_symbol'].values[0]

    # start of event
    if monitoring_type != 'event' and len(pos_trig) != 0:
        ts_onset = min(pos_trig['ts'].values)
    
    # most recent retrigger of positive operational triggers
    try:
        #last positive retriggger/s
        triggers = last_pos_trig[['alert_symbol', 'ts_updated']]
        triggers = triggers.rename(columns = {'alert_symbol': 'alert', \
                'ts_updated': 'ts'})
        triggers['ts'] = triggers['ts'].apply(lambda x: str(x))
    except:
        triggers = pd.DataFrame(columns=['alert', 'ts'])

    #technical info for bulletin release
    try:
        tech_info = pd.DataFrame(columns=['subsurface', 'surficial', 'rainfall', \
            'earthquake', 'on demand'])
    except:
        tech_info = pd.DataFrame()
    
    try:    
        ts = max(op_trig[op_trig.alert_level != 0]['ts_updated'].values)
        ts = release_time(pd.to_datetime(ts))
    except:
        ts = end
    ts = str(ts)
    
    validity = str(validity)
    
    public_df = pd.DataFrame({'ts': [ts], 'site_code': [site_code], \
            'public_alert': [public_alert], 'internal_alert': [internal_alert], \
            'validity': [validity], 'subsurface': [subsurface], \
            'surficial': [surficial], 'rainfall': [rainfall], \
            'triggers': [triggers], 'tech_info': [tech_info]})
    
    # writes public alert to database
    pub_sym_id =  public_symbols[public_symbols.alert_level == \
                  public_alert]['pub_sym_id'].values[0]
    site_public_df = pd.DataFrame({'ts': [end], 'site_id': [site_id], \
            'pub_sym_id': [pub_sym_id], 'ts_updated': [end]})
    
    # onset trigger
    try:
        site_public_df['ts'] = ts_onset
    except:
        pass
    
#    qdb.alert_to_db(site_public_df, 'public_alerts')
    
    public_df['public_alert'] = public_symbols[public_symbols.alert_level == \
                             public_alert]['alert_symbol'].values[0]

    return public_df

def main(end=datetime.now()):
    """Compiles all alerts to compute for public alert and internal alert.
    Writes result to public_alert table and publicalert.json

    Args:
        end (datetime): Optional. Public alert timestamp.
    """
    start_time = datetime.now()
    qdb.print_out(start_time)

    end = data_ts(pd.to_datetime(end))
    
    # alert symbols
    public_symbols = get_public_symbols()     
    internal_symbols = get_internal_symbols()
    trig_symbols = get_trigger_symbols()

    # site id and code
    query = "SELECT site_id, site_code FROM sites WHERE site_code != 'mes'"
    props = qdb.get_db_dataframe(query)

    site_props = props.groupby('site_id', as_index=False)
    
    alerts = site_props.apply(site_public_alert, end=end,
                              public_symbols=public_symbols,
                              internal_symbols=internal_symbols, 
                              trig_symbols=trig_symbols, start_time=start_time)

    alerts['cat'] = pd.Categorical(alerts['public_alert'],
              categories=public_symbols['alert_symbol'].values, ordered=True)
    alerts = alerts.sort_values(['cat', 'site_code']).drop('cat', axis=1)
    
    all_alerts = pd.DataFrame({'invalids': [np.nan], 'alerts': [alerts]})
    
    public_json = all_alerts.to_json(orient="records")

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sc = qdb.memcached()
    if not os.path.exists(output_path+sc['fileio']['output_path']):
        os.makedirs(output_path+sc['fileio']['output_path'])

    with open(output_path+sc['fileio']['output_path']+'PublicAlertRefDB.json', 'w') as w:
        w.write(public_json)

    qdb.print_out('runtime = %s' %(datetime.now() - start_time))
    
    return alerts

################################################################################

if __name__ == "__main__":
    main()