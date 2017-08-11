from datetime import datetime, timedelta, time, date
import numpy as np
import os
import pandas as pd

import querydb as qdb

def release_time(date_time):
    # rounds time to 4/8/12 AM/PM
    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4
    if quotient == 5:
        date_time = datetime.combine(date_time.date() + timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time

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

def old_internal(df, routine):
    pub = df['public_alert'].values[0]
    internal = df['internal_alert'].values[0]
    if pub not in routine:
        add = '-'
    else:
        add = ''
    df['internal_alert'] = pub[0:2] + add+ internal
    return df

def internal_alert_symbol(positive_trigger, optrigger_withdata, intsym, 
                          rainfall_alert):
    highest_trigger = positive_trigger.sort_values('alert_level', \
            ascending=False).drop_duplicates('trigger_source')
    
    # arrange trigger_source
    trigger_source = []
    if 'subsurface' in highest_trigger['trigger_source'].values:
        trigger_source += ['subsurface']
    if 'surficial' in highest_trigger['trigger_source'].values:
        trigger_source += ['surficial']
    if 'movement' in highest_trigger['trigger_source'].values:
        trigger_source += ['movement']
    if 'rainfall' in highest_trigger['trigger_source'].values:
        trigger_source += ['rainfall']
    if 'earthquake' in highest_trigger['trigger_source'].values:
        trigger_source += ['earthquake']
    if 'on demand' in highest_trigger['trigger_source'].values:
        trigger_source += ['on demand']

    internal_alert = ''
    for i in trigger_source:
        alert_level = highest_trigger[highest_trigger.trigger_source == i]\
                ['alert_level'].values[0]
        if i in ['movement', 'earthquake', 'on demand']:
            internal_alert += intsym[(intsym.trigger_source == i) & \
                    (intsym.alert_level == alert_level)]\
                    ['alert_symbol'].values[0]
        elif i == 'rainfall' and rainfall_alert == -2:
            internal_alert += intsym[(intsym.trigger_source == i) & \
                    (intsym.alert_level == -2)]['alert_symbol'].values[0]
        else:
            if i in optrigger_withdata['trigger_source'].values or \
                    (i == 'rainfall' and rainfall_alert != -1):
                internal_alert += intsym[(intsym.trigger_source == i) & \
                        (intsym.alert_level == alert_level)]\
                        ['alert_symbol'].values[0]
            elif i != 'rainfall' and alert_level < 3:
                internal_alert += intsym[(intsym.trigger_source == i) & \
                        (intsym.alert_level == -1)]['alert_symbol'].\
                        values[0].lower()
            else:
                internal_alert += intsym[(intsym.trigger_source == i) & \
                        (intsym.alert_level == -1)]['alert_symbol'].values[0]
    if 'rainfall' not in trigger_source and rainfall_alert == -2:
        internal_alert += intsym[(intsym.trigger_source == 'rainfall') & \
                (intsym.alert_level == -2)]['alert_symbol'].values[0].lower()

    return internal_alert

def site_public_alert(PublicAlert, end, pubsym, intsym, opsym):
    site_code = PublicAlert['site_code'].values[0]
    site_id = PublicAlert['site_id'].values[0]
    qdb.print_out(site_code)

    if qdb.does_table_exist('public_alerts') == False:
        #Create a public_alerts table if it doesn't exist yet
        qdb.create_public_alerts()
        
    query = "SELECT ts, site_id, alert_level, alert_type, alert_symbol, ts_updated FROM"
    query += " (SELECT * FROM public_alerts WHERE site_id = %s" %site_id
    query += " AND ts <= '%s' AND ts_updated >= '%s') pub" %(end, \
            end - timedelta(hours=0.5))
    query += " INNER JOIN public_alert_symbols AS sym"
    query += " ON pub.pub_sym_id = sym.pub_sym_id"
    query += " ORDER BY ts DESC LIMIT 1"
    
    # previous public alert
    PubAlert = qdb.get_db_dataframe(query)
    try:
        PrevPubAlert = PubAlert['alert_level'].values[0]
    except:
        PrevPubAlert = 0
    
    # with previous positive alert
    if PrevPubAlert > 0:
        query = "SELECT ts, site_id, alert_level, alert_symbol, ts_updated FROM"
        query += " (SELECT * FROM public_alerts"
        query += " WHERE site_id = %s) pub" %site_id
        query += " INNER JOIN"
        query += " (SELECT * FROM public_alert_symbols"
        query += " WHERE alert_level > 0) sym"
        query += " ON pub.pub_sym_id = sym.pub_sym_id"
        query += " ORDER BY ts DESC LIMIT 3"
        
        # previous positive alert
        PosPubAlert = qdb.get_db_dataframe(query)
        
        if len(PosPubAlert) == 1:
            start_monitor = pd.to_datetime(PosPubAlert['ts'].values[0])
        # two previous positive alert
        elif len(PosPubAlert) == 2:
            # one event with two previous positive alert
            if pd.to_datetime(PosPubAlert['ts'].values[0]) - \
                    pd.to_datetime(PosPubAlert['ts_updated'].values[1]) <= \
                    timedelta(hours=0.5):
                start_monitor = pd.to_datetime(PosPubAlert['ts'].values[1])
            else:
                start_monitor = pd.to_datetime(PosPubAlert['ts'].values[0])
        # three previous positive alert
        else:
            if pd.to_datetime(PosPubAlert['ts'].values[0]) - \
                    pd.to_datetime(PosPubAlert['ts_updated'].values[1]) <= \
                    timedelta(hours=0.5):
                # one event with three previous positive alert
                if pd.to_datetime(PosPubAlert['ts'].values[1]) - \
                        pd.to_datetime(PosPubAlert['ts_updated'].values[2]) \
                        <= timedelta(hours=0.5):
                    start_monitor = pd.to_datetime(PosPubAlert['timestamp']\
                            .values[2])
                # one event with two previous positive alert
                else:
                    start_monitor = pd.to_datetime(PosPubAlert['ts'].values[1])
            else:
                start_monitor = pd.to_datetime(PosPubAlert['ts'].values[0])
    # previous public alert level 0
    else:
        start_monitor = pd.to_datetime(end.date())

    query = "SELECT ts, site_id, trigger_source,"
    query += " alert_level, alert_symbol, ts_updated FROM"
    query += " (SELECT * FROM operational_triggers WHERE site_id = %s" %site_id
    query += " AND ts_updated >= '%s' AND ts <= '%s') op" %(start_monitor, end)
    query += " INNER JOIN operational_trigger_symbols as sym"
    query += " ON op.trigger_sym_id = sym.trigger_sym_id"

    # operational triggers from start of monitoring
    op_trigger = qdb.get_db_dataframe(query)
    op_trigger = op_trigger.sort_values('ts', ascending=False)
    # operational triggers ts after previous release
    recent_op_trigger = op_trigger[op_trigger.ts_updated >= \
            release_time(end)-timedelta(hours=4)]
    recent_op_trigger = recent_op_trigger.drop_duplicates(['trigger_source', \
            'alert_level'])
    # positive operational triggers from start of monitoring
    positive_trigger = op_trigger[op_trigger.alert_level > 0]
    # most recent positive operational triggers
    last_positive_trigger = positive_trigger.drop_duplicates(['trigger_source', \
            'alert_level'])
    # with ground data
    if PubAlert['alert_type'].values[0] == 'routine':
        surficial_ts = start_monitor
    else:
        surficial_ts = release_time(end)-timedelta(hours=4)
    with_ground_data = op_trigger[(op_trigger.alert_level != -1) & \
            ( ((op_trigger.trigger_source == 'surficial') & \
               (op_trigger.ts_updated >= surficial_ts)) | \
              ((op_trigger.trigger_source == 'subsurface') & \
               (op_trigger.ts_updated >= end)) )]

    if PubAlert['alert_type'].values[0] == 'routine' and len(positive_trigger) != 0:
        ts_onset = min(positive_trigger['ts'].values)
    
    # most recent retrigger of positive operational triggers
    try:
        #last positive retriggger/s
        triggers = last_positive_trigger[['alert_symbol', 'ts_updated']]
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
    
    # most recent tsm alert ts after previous release
    query = "SELECT ts, tsm_name, alert_symbol FROM"
    query += "   (SELECT ts, tsm_name, alert_level FROM"
    query += "   (SELECT * FROM tsm_alerts WHERE"
    query += "   (ts <= '%s' AND ts_updated >= '%s')" %(end, end)
    query += "   OR (ts <= '%s' AND ts_updated >= '%s')) a" %(data_ts(end) \
            -timedelta(hours=4), data_ts(end)-timedelta(hours=4))
    query += "   INNER JOIN"
    query += "   (SELECT * FROM tsm_sensors WHERE site_id = %s) tsm" %site_id
    query += "   ON tsm.tsm_id = a.tsm_id) AS sub"
    query += " INNER JOIN (SELECT * FROM operational_trigger_symbols"
    query += " WHERE trigger_source = 'subsurface') sym"
    query += " ON sub.alert_level = sym.alert_level"

    subsurface = qdb.get_db_dataframe(query)
    subsurface = subsurface.sort_values('ts', ascending=False)
    subsurface = subsurface.drop_duplicates('tsm_name')
    subsurface = subsurface.rename(columns = {'alert_symbol': 'alert'})
    subsurface = subsurface[['tsm_name', 'alert']]
    
    # most recent rainfall alert ts after previous release
    try:
        rainfall = recent_op_trigger[recent_op_trigger.trigger_source == \
                'rainfall']['alert_symbol'].values[0]
        rainfall_alert = recent_op_trigger[recent_op_trigger.trigger_source == \
                'rainfall']['alert_level'].values[0]
        if PrevPubAlert > 0:
            query = "SELECT * FROM senslopedb.rainfall_alerts"
            query += " WHERE site_id = %s AND ts = '%s'" %(site_id, end)
            query +=" AND rain_alert = '0'"
            try:
                if len(qdb.get_db_dataframe(query)) == 0 and end.time() in \
                        [time(3,30), time(7,30), time(11,30), time(15,30), \
                        time(19,30), time(23,30)]:
                    extend_rainfall = True
                    rainfall = opsym[(opsym.alert_level == -2) & \
                            (opsym.trigger_source == 'rainfall')]\
                            ['alert_symbol'].values[0]
                    rainfall_alert = -2
                else:
                    extend_rainfall = False
            except:
                pass
    except:
        extend_rainfall = False
        rainfall = opsym[(opsym.alert_level == -1)&(opsym.trigger_source == \
                'rainfall')]['alert_symbol'].values[0]
        rainfall_alert = -1
    
    # most recent surficial alert ts after previous release
    try:
        surficial = with_ground_data[(with_ground_data.trigger_source == \
                'surficial')]['alert_symbol'].values[0]
    except:
        surficial = opsym[(opsym.alert_level == -1)&(opsym.trigger_source == \
                'surficial')]['alert_symbol'].values[0]

    # internal alert
    internal_alert = internal_alert_symbol(positive_trigger, recent_op_trigger, \
            intsym, rainfall_alert)
    internal_alerts_df = pd.DataFrame({'ts': [end], 'site_id': [site_id], \
            'internal_sym': [internal_alert], 'ts_updated': [end]})
    qdb.alert_to_db(internal_alerts_df, 'internal_alerts')

    # checks if surficial and subsurface triggers have current alert level 0
    withdata = []
    if PrevPubAlert == 3 or (PrevPubAlert == 2 and ('subsurface' in \
            positive_trigger['trigger_source'].values or 'surficial' in \
            positive_trigger['trigger_source'].values)):
        for i in ['subsurface', 'surficial']:
            if i in positive_trigger['trigger_source'].values:
                if i not in with_ground_data['trigger_source'].values:
                     withdata += [False]
    elif len(with_ground_data) == 0:
        withdata += [False]
    withdata = all(withdata)

    #checks for new positive trigger:
    try:
        if max(recent_op_trigger['alert_level'].values) > 0:
            new_trig = True
        else:
            new_trig = False
    except:
        new_trig = False

    # Public Alert > 0
    if PrevPubAlert > 0 or new_trig:
        validity_op = list(positive_trigger['ts_updated'].values)
        validity_pub = list(PubAlert['ts'].values)
        validity = release_time(pd.to_datetime(max(validity_pub + validity_op)))
        if PrevPubAlert == 3:
            validity += timedelta(2)
        else:
            validity += timedelta(1)
        
        # Public Alert is still valid
        if validity > end + timedelta(hours=0.5):
            CurrAlert = max(list(recent_op_trigger['alert_level'].values) + [PrevPubAlert])
            if CurrAlert == 1 and not withdata:
                CurrAlert = -1
            public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
            pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
            
        # end of Public Alert validity
        else:            
            # with data
            if withdata:
                #if rainfall above 75%
                if extend_rainfall:
                    CurrAlert = max(list(recent_op_trigger['alert_level'].values) + [PrevPubAlert])
                    public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                    pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                    validity = release_time(end)
                else:
                    # if rainfall alert -1
                    if rainfall_alert == -1: #### nd rainfall after r1 extend
                        query = "SELECT alert_level FROM (SELECT * FROM operational_triggers"
                        query += " WHERE site_id = %s AND ts <= '%s') op" %(site_id, end)
                        query += " INNER JOIN (SELECT * FROM operational_trigger_symbols"
                        query += " WHERE trigger_source = 'rainfall') sym"
                        query += " ON op.trigger_sym_id = sym.trigger_sym_id"
                        query += " ORDER BY ts DESC LIMIT 2"
                        # if from rainfall alert 1
                        if qdb.get_db_dataframe(query).values[0][0] == 1:
                            # within 1-day cap of 4H extension for nd                                
                            if release_time(end) - validity < timedelta(1):
                                CurrAlert = max(list(recent_op_trigger['alert_level'].values) + [PrevPubAlert])
                                public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                                pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                                validity = release_time(end)
                            # end of 1-day cap of 4H extension for nd
                            else:
                                CurrAlert = 0
                                public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                                pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                                validity = ''
                        else:
                            CurrAlert = 0
                            public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                            pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                            validity = ''
                    else:
                        CurrAlert = 0
                        public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                        pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                        validity = ''
            # without data
            else:
                # within 3 days of 4hr-extension                    
                if (release_time(end) - validity < timedelta(3)) or  extend_rainfall:
                    CurrAlert = -1
                    public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                    pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                    validity = release_time(end)
                else:
                    CurrAlert = -1
                    public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                    pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                    validity = ''                  

    #Public Alert A0
    else:
        if not withdata:
            CurrAlert = -1
        else:
            CurrAlert = 0
        
        public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'routine')]['alert_symbol'].values[0]
        pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'routine')]['pub_sym_id'].values[0]
        validity = ''
        
    # latest timestamp of data
    if len(recent_op_trigger) != 0:
        ts = data_ts(pd.to_datetime(max(recent_op_trigger['ts_updated'].values)))
        if ts > end:
            ts = end
    else:
        ts = end
        
    PublicAlert = pd.DataFrame({'ts': [ts], 'site_code': [site_code], \
            'public_alert': [public_alert], 'internal_alert': [internal_alert], \
            'validity': [validity], 'subsurface': [subsurface], \
            'surficial': [surficial], 'rainfall': [rainfall], \
            'triggers': [triggers], 'tech_info': [tech_info]})
        
    SitePublicAlert = pd.DataFrame({'ts': [end], 'site_id': [site_id], \
            'pub_sym_id': [pub_sym_id], 'ts_updated': [end]})
    # suficial alert onset trigger
    try:
        SitePublicAlert['ts'] = ts_onset
    except:
        pass
    
    qdb.alert_to_db(SitePublicAlert, 'public_alerts')

    return PublicAlert

def main(end=datetime.now()):
    start_time = datetime.now()
    qdb.print_out(start_time)

    end = data_ts(pd.to_datetime(end))
    
    query = "SELECT * FROM public_alert_symbols"
    PublicAlertSymbols = qdb.get_db_dataframe(query)
    PublicAlertSymbols = PublicAlertSymbols.sort_values(['alert_type', \
            'alert_level'], ascending=[True, False])
    
    query = "SELECT i.alert_symbol, alert_level, trigger_source FROM"
    query += " internal_alert_symbols AS i"
    query += " INNER JOIN operational_trigger_symbols AS op"
    query += " ON op.trigger_sym_id = i.trigger_sym_id"
    InternalAlertSymbols = qdb.get_db_dataframe(query)
    
    query = "SELECT * FROM operational_trigger_symbols"
    OperationalTriggerSymbols = qdb.get_db_dataframe(query)
    
    query = "SELECT site_id, site_code FROM sites WHERE site_code != 'mes'"
    PublicAlert = qdb.get_db_dataframe(query)

    Site_Public_Alert = PublicAlert.groupby('site_id', as_index=False)
    
    PublicAlert = Site_Public_Alert.apply(site_public_alert, end=end, \
            pubsym=PublicAlertSymbols, intsym=InternalAlertSymbols, \
            opsym=OperationalTriggerSymbols)
    PublicAlert['cat'] = pd.Categorical(PublicAlert['public_alert'], \
            categories=PublicAlertSymbols['alert_symbol'].values, ordered=True)
    PublicAlert = PublicAlert.sort_values(['cat', 'site_code']).drop('cat', axis=1)
 
    PublicAlert['ts'] = PublicAlert['ts'].apply(lambda x: str(x))
    PublicAlert['validity'] = PublicAlert['validity'].apply(lambda x: str(x))
    
    #################### transform public_alert and internal_alert #############
    #################### to be similar to dyna public alert ####################
    
    PublicAlertSymbols.loc[PublicAlertSymbols.alert_type == 'routine', ['alert_level']] = 0
    PublicAlertSymbols['alert_level'] = np.abs(PublicAlertSymbols['alert_level']).apply(lambda x: 'A'+str(x))
    pub_map = PublicAlertSymbols[['alert_symbol', 'alert_level']].set_index('alert_symbol').to_dict()['alert_level']

    SitePublicAlert = PublicAlert.groupby('site_code')
    routine = PublicAlertSymbols[PublicAlertSymbols.alert_type == 'routine']['alert_symbol'].values
    PublicAlert = SitePublicAlert.apply(old_internal, routine=routine)
    PublicAlert['public_alert'] = PublicAlert['public_alert'].map(pub_map)
    
    ############################################################################
    
    all_alerts = pd.DataFrame({'invalids': [np.nan], 'alerts': [PublicAlert]})
    
    public_json = all_alerts.to_json(orient="records")

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sc = qdb.memcached()
    if not os.path.exists(output_path+sc['fileio']['output_path']):
        os.makedirs(output_path+sc['fileio']['output_path'])

    with open(output_path+sc['fileio']['output_path']+'PublicAlertRefDB.json', 'w') as w:
        w.write(public_json)

    qdb.print_out('runtime = %s' %(datetime.now() - start_time))

    return PublicAlert

################################################################################

if __name__ == "__main__":
    main()
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sc = qdb.memcached()
    df = pd.DataFrame(pd.read_json(output_path+sc['fileio']['output_path']+'PublicAlertRefDB.json')['alerts'][0])
