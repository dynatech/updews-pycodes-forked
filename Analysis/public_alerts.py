from datetime import datetime, timedelta, time, date
import os
import pandas as pd

import configfileio as cfg
import querydb as q

def RoundReleaseTime(date_time):
    # rounds time to 4/8/12 AM/PM
    time_hour = int(date_time.strftime('%H'))

    quotient = time_hour / 4
    if quotient == 5:
        date_time = datetime.combine(date_time.date() + timedelta(1), time(0,0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0,0))
            
    return date_time

def RoundDataTS(endpt):
    end_Year=endpt.year
    end_month=endpt.month
    end_day=endpt.day
    end_hour=endpt.hour
    end_minute=endpt.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))
    return end

def InternalAlert(positive_trigger, optrigger_withdata, intsym, rainfall_alert):
    highest_trigger = positive_trigger.sort_values('alert_level', ascending=False).drop_duplicates('trigger_source')

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
        alert_level = highest_trigger[highest_trigger.trigger_source == i]['alert_level'].values[0]
        if i == 'rainfall' and rainfall_alert == -2:
            internal_alert += intsym[(intsym.trigger_source == i)&(intsym.alert_level == -2)]['alert_symbol'].values[0]
        else:
            if i in optrigger_withdata['trigger_source'].values:
                internal_alert += intsym[(intsym.trigger_source == i)&(intsym.alert_level == alert_level)]['alert_symbol'].values[0]
            else:
                internal_alert += intsym[(intsym.trigger_source == i)&(intsym.alert_level == -alert_level)]['alert_symbol'].values[0]
    if 'rainfall' not in trigger_source and rainfall_alert == -2:
        internal_alert += intsym[(intsym.trigger_source == 'rainfall')&(intsym.alert_level == -2)]['alert_symbol'].values[0].lower()

    return internal_alert

def SitePublicAlert(PublicAlert, end, pubsym, intsym, opsym):
    site_code = PublicAlert['site_code'].values[0]
    site_id = PublicAlert['site_id'].values[0]
    print site_code

    if q.DoesTableExist('public_alerts') == False:
        #Create a public_alerts table if it doesn't exist yet
        q.create_public_alerts()
        
    query = "SELECT ts, site_id, alert_level, alert_symbol, ts_updated FROM"
    query += " (SELECT * FROM public_alerts WHERE site_id = %s" %site_id
    query += " AND ts <= '%s' AND ts_updated >= '%s') pub" %(end, end-timedelta(hours=0.5))
    query += " INNER JOIN public_alert_symbols AS sym"
    query += " ON pub.pub_sym_id = sym.pub_sym_id"
    query += " ORDER BY ts DESC LIMIT 1"
    
    # previous public alert
    PubAlert = q.GetDBDataFrame(query)
    try:
        PrevPubAlert = PubAlert['alert_level'].values[0]
    except:
        PrevPubAlert = 0
    
    # with previous positive alert
    if PrevPubAlert > 0:
        positive_alert = True
        
        query = "SELECT ts, site_id, alert_level, alert_symbol, ts_updated FROM"
        query += " (SELECT * FROM public_alerts WHERE site_id = %s) pub" %site_id
        query += " INNER JOIN (SELECT * FROM public_alert_symbols  WHERE alert_level > 0) sym"
        query += " ON pub.pub_sym_id = sym.pub_sym_id"
        query += " ORDER BY ts DESC LIMIT 3"
        
        # previous positive alert
        PosPubAlert = q.GetDBDataFrame(query)
        
        if len(PosPubAlert) == 1:
            start_monitor = pd.to_datetime(PosPubAlert['ts'].values[0])
        # two previous positive alert
        elif len(PosPubAlert) == 2:
            # one event with two previous positive alert
            if pd.to_datetime(PosPubAlert['ts'].values[0]) - pd.to_datetime(PosPubAlert['ts_updated'].values[1]) <= timedelta(hours=0.5):
                start_monitor = pd.to_datetime(PosPubAlert['ts'].values[1])
            else:
                start_monitor = pd.to_datetime(PosPubAlert['ts'].values[0])
        # three previous positive alert
        else:
            if pd.to_datetime(PosPubAlert['ts'].values[0]) - pd.to_datetime(PosPubAlert['ts_updated'].values[1]) <= timedelta(hours=0.5):
                # one event with three previous positive alert
                if pd.to_datetime(PosPubAlert['ts'].values[1]) - pd.to_datetime(PosPubAlert['ts_updated'].values[2]) <= timedelta(hours=0.5):
                    start_monitor = pd.to_datetime(PosPubAlert.timestamp.values[2])
                # one event with two previous positive alert
                else:
                    start_monitor = pd.to_datetime(PosPubAlert['ts'].values[1])
            else:
                start_monitor = pd.to_datetime(PosPubAlert['ts'].values[0])
    # previous public alert level 0
    else:
        positive_alert = False

    query = "SELECT ts, site_id, trigger_source, alert_level, alert_symbol, ts_updated FROM"
    query += " (SELECT * FROM operational_triggers WHERE site_id = %s" %site_id

    # operational triggers from start of positive alert
    if positive_alert:
        query += " AND ((ts >= '%s' AND ts <= '%s')" %(start_monitor-timedelta(hours=0.5), end)        
        query += " OR (ts <= '%s' AND ts_updated >= '%s'))) op" %(start_monitor-timedelta(hours=0.5), start_monitor-timedelta(hours=0.5))
    # operational trigger from ts after previous release
    else:
        query += " AND ((ts <= '%s' AND ts_updated >= '%s')" %(end, end)        
        query += " OR (ts <= '%s' AND ts_updated >= '%s'))) op" %(RoundReleaseTime(end)-timedelta(hours=4), RoundReleaseTime(end)-timedelta(hours=4))

    query += " INNER JOIN operational_trigger_symbols as sym"
    query += " ON op.trigger_sym_id = sym.trigger_sym_id"

    # operational triggers
    operational_trigger = q.GetDBDataFrame(query)
    operational_trigger = operational_trigger.sort_values('ts', ascending=False)
    # operational triggers ts after previous release
    recent_op_trigger = operational_trigger[operational_trigger.ts_updated >= RoundReleaseTime(end)-timedelta(hours=4)]
    # positive operational triggers
    positive_trigger = operational_trigger[operational_trigger.alert_level > 0]
    # most recent positive operational triggers
    last_positive_trigger = positive_trigger.drop_duplicates(['trigger_source', 'alert_level'])
    # operational triggers with data
    optrigger_withdata = recent_op_trigger[recent_op_trigger.alert_level >= 0]

    if not positive_alert and len(operational_trigger[(operational_trigger.alert_level > 0)&(operational_trigger.trigger_source == 'surficial')]) != 0:
        ts_onset = operational_trigger[operational_trigger.trigger_source == 'surficial']['ts'].values[0] +timedelta(hours=0.5)
    
    # most recent retrigger of positive operational triggers
    try:
        #last L2/L3 retriggger
        triggers = last_positive_trigger[['alert_symbol', 'ts_updated']]
        triggers = triggers.rename(columns = {'alert_symbol': 'alert', 'ts_updated': 'ts'})
        triggers['ts'] = triggers['ts'].apply(lambda x: str(x))
    except:
        triggers = pd.DataFrame(columns=['alert', 'ts'])

    #technical info for bulletin release
    tech_info = pd.DataFrame(columns=['subsurface', 'surficial', 'rainfall', 'earthquake', 'on demand'])
    
    # most recent tsm alert ts after previous release
    query = "SELECT ts, tsm_name, alert_symbol FROM"
    query += "   (SELECT ts, tsm_name, alert_level FROM"
    query += "   (SELECT * FROM tsm_alerts WHERE (ts <= '%s' AND ts_updated >= '%s')" %(end, end)
    query += "   OR (ts <= '%s' AND ts_updated >= '%s')) a"  %(RoundReleaseTime(end)-timedelta(hours=4), RoundReleaseTime(end)-timedelta(hours=4))
    query += "   INNER JOIN (SELECT * FROM tsm_sensors WHERE site_id = %s) tsm" %site_id
    query += "   ON tsm.tsm_id = a.tsm_id) AS sub"
    query += " INNER JOIN (SELECT * FROM operational_trigger_symbols"
    query += " WHERE trigger_source = 'subsurface') sym ON sub.alert_level = sym.alert_level"

    subsurface = q.GetDBDataFrame(query)
    subsurface = subsurface.sort_values('ts', ascending=False).drop_duplicates('tsm_name')
    subsurface = subsurface.rename(columns = {'alert_symbol': 'alert'})
    subsurface = subsurface[['tsm_name', 'alert']]
    
    # most recent rainfall alert ts after previous release
    try:
        rainfall = recent_op_trigger[recent_op_trigger.trigger_source == 'rainfall']['alert_symbol'].values[0]
        rainfall_alert = recent_op_trigger[recent_op_trigger.trigger_source == 'rainfall']['alert_level'].values[0]
        if PrevPubAlert > 0:
            query = "SELECT * FROM senslopedb.rainfall_alerts"
            query += " WHERE site_id = %s AND ts = '%s' AND rain_alert = '0'" %(site_id, end)
            try:
                if len(q.GetDBDataFrame(query)) == 0 and end.time() in [time(3,30), time(7,30), time(11,30), time(15,30), time(19,30), time(23,30)]:
                    extend_rainfall = True
                else:
                    extend_rainfall = False
            except:
                pass
    except:
        extend_rainfall = False
        rainfall = opsym[(opsym.alert_level==-1)&(opsym.trigger_source=='rainfall')]['alert_symbol'].values[0]
        rainfall_alert = -1
    
    # most recent surficial alert ts after previous release
    try:
        surficial = recent_op_trigger[(recent_op_trigger.trigger_source == 'surficial')]['alert_symbol'].values[0]
    except:
        surficial = opsym[(opsym.alert_level==-1)&(opsym.trigger_source=='surficial')]['alert_symbol'].values[0]

    # internal alert
    internal_alert = InternalAlert(positive_trigger, recent_op_trigger, intsym, rainfall_alert)
    internal_alerts_df = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'internal_sym': [internal_alert], 'ts_updated': [end]})
    q.alert_toDB(internal_alerts_df, 'internal_alerts')

    # checks if surficial and subsurface triggers have current alert level 0
    withdata = []
    if PrevPubAlert == 3 or (PrevPubAlert == 2 and ('subsurface' in positive_trigger['trigger_source'].values or 'surficial' in positive_trigger['trigger_source'].values)):
        for i in ['subsurface', 'surficial']:
            if i in positive_trigger['trigger_source'].values:
                if i not in optrigger_withdata['trigger_source'].values:
                     withdata += [False]
    elif 'subsurface' not in optrigger_withdata['trigger_source'].values and 'surficial' not in optrigger_withdata['trigger_source'].values:
        withdata += [False]
    withdata = all(withdata)

    # Public Alert > 0
    if PrevPubAlert > 0 or max(recent_op_trigger['alert_level'].values) > 0:
        validity_op = list(positive_trigger['ts_updated'].values)
        validity_pub = list(PubAlert['ts'].values)
        validity = RoundReleaseTime(pd.to_datetime(max(validity_pub + validity_op)))
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
                    validity = RoundReleaseTime(end)
                    rainfall = opsym[(opsym.alert_level==-2)&(opsym.trigger_source=='rainfall')]['alert_symbol'].values[0]
                    rainfall_alert = -2
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
                        if q.GetDBDataFrame(query).values[0][0] == 1:
                            # within 1-day cap of 4H extension for nd                                
                            if RoundReleaseTime(end) - validity < timedelta(1):
                                CurrAlert = max(list(recent_op_trigger['alert_level'].values) + [PrevPubAlert])
                                public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                                pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                                validity = RoundReleaseTime(end)
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
                if (RoundReleaseTime(end) - validity < timedelta(3)) or  extend_rainfall:
                    CurrAlert = -1
                    public_alert = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['alert_symbol'].values[0]
                    pub_sym_id = pubsym[(pubsym.alert_level == CurrAlert)&(pubsym.alert_type == 'event')]['pub_sym_id'].values[0]
                    validity = RoundReleaseTime(end)
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
        ts = pd.to_datetime(max(recent_op_trigger['ts_updated'].values))
        if ts > end:
            ts = end
    else:
        ts = end
        
    PublicAlert = pd.DataFrame({'ts': [ts], 'site_code': [site_code], 'public_alert': [public_alert], 'internal_alert': [internal_alert], 'validity': [validity], 'subsurface': [subsurface], 'surficial': [surficial], 'rainfall': [rainfall], 'triggers': [triggers], 'tech_info': [tech_info]})
        
    SitePublicAlert = pd.DataFrame({'ts': [end], 'site_id': [site_id], 'pub_sym_id': [pub_sym_id], 'ts_updated': [end]})
    # suficial alert onset trigger
    try:
        SitePublicAlert['ts'] = ts_onset
    except:
        pass
    
    q.alert_toDB(SitePublicAlert, 'public_alerts')

    return PublicAlert

def main(end=datetime.now()):
    end = RoundDataTS(pd.to_datetime(end))
    
    query = "SELECT * FROM public_alert_symbols"
    PublicAlertSymbols = q.GetDBDataFrame(query)
    
    query = "SELECT i.alert_symbol, alert_level, trigger_source FROM"
    query += " internal_alert_symbols AS i"
    query += " INNER JOIN operational_trigger_symbols AS op"
    query += " ON op.trigger_sym_id = i.trigger_sym_id"
    InternalAlertSymbols = q.GetDBDataFrame(query)
    
    query = "SELECT * FROM operational_trigger_symbols"
    OperationalTriggerSymbols = q.GetDBDataFrame(query)
    
    query = "SELECT site_id, site_code FROM sites WHERE site_code != 'mes'"
    PublicAlert = q.GetDBDataFrame(query)

    Site_Public_Alert = PublicAlert.groupby('site_id', as_index=False)
    
    PublicAlert = Site_Public_Alert.apply(SitePublicAlert, end=end, pubsym=PublicAlertSymbols, intsym=InternalAlertSymbols, opsym=OperationalTriggerSymbols)
    PublicAlert = PublicAlert.sort_values(['public_alert', 'site_code'], ascending = [False, True])
 
    PublicAlert['ts'] = PublicAlert['ts'].apply(lambda x: str(x))
    PublicAlert['validity'] = PublicAlert['validity'].apply(lambda x: str(x))

    public_json = PublicAlert.to_json(orient="records")

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    s = cfg.config()

    with open(output_path+s.io.outputfilepath+'PublicAlertRefDB.json', 'w') as w:
        w.write(public_json)
                
    return PublicAlert

################################################################################

if __name__ == "__main__":
    start_time = datetime.now()
    print start_time
    main()
    print 'runtime =', datetime.now() - start_time