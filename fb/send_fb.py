# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 11:28:19 2018

@author: Brainerd Cruz
"""

import analysis.querydb as qdb
import pandas as pd
from datetime import timedelta as td
import os
import xyzrealtimeplot as xyz
import analysis.subsurface.filterdata as fsd
from fbchat import Client
from fbchat.models import *

def main(alert):    

    site = alert.site_code
    ts = alert.ts_last_retrigger
    
    OutputFP=  os.path.abspath(os.path.join(os.path.dirname(__file__), '../')) #os.path.dirname(os.path.realpath(__file__))+'/{} {}/'.format(site, ts.strftime("%Y-%m-%d %H%M"))
    OutputFP += '/node alert validation sandbox/' + '{} {}/'.format(site, ts.strftime("%Y-%m-%d %H%M")) 
    OutputFP=OutputFP.replace("\\", "/")
    
    if not os.path.exists(OutputFP):
        os.makedirs(OutputFP)
    
    ts_before=ts.round('4H')-td(hours=4)
    
    queryalert="""SELECT na_id,ts,t.tsm_id,tsm_name,node_id,disp_alert,vel_alert 
                FROM senslopedb.node_alerts
                inner join tsm_sensors as t
                on t.tsm_id=node_alerts.tsm_id
                where site_id={} and (ts between '{}' and '{}')

                order by tsm_name, node_id, ts desc""".format(alert.site_id,ts_before,ts)
    dfalert=qdb.get_db_dataframe(queryalert).groupby(['tsm_id','node_id']).first().reset_index()
    
    for i in dfalert.index:
        print dfalert.tsm_name[i],dfalert.node_id[i],dfalert.ts[i]
        
        df_node=qdb.get_raw_accel_data(tsm_id=dfalert.tsm_id[i],
                                    from_time=dfalert.ts[i]-td(weeks=1),
                                    to_time=dfalert.ts[i])
        dff=fsd.apply_filters(df_node)
        
        raw_count = float(dff.ts[(dff.node_id==dfalert.node_id[i]) & 
                              (dff.ts>=dfalert.ts[i]-td(days=3))].count())
        filter_count = df_node.ts[(df_node.node_id==dfalert.node_id[i]) & 
                                  (dff.ts>=dfalert.ts[i]-td(days=3))].count()
        
        percent= raw_count / filter_count * 100.0

        xyz.xyzplot(dff,dfalert.tsm_id[i],dfalert.node_id[i],dfalert.ts[i],OutputFP,percent)
    return OutputFP

def send_messenger(OutputFP, alert):
    client = Client('dynaslope.test@gmail.com', 'dynaslope00')
    
    message=("SANDBOX:\n"
            "As of {}\n"
            "Alert ID {}:\n"
            "{}:{}:{}\n\n".format(alert.ts_last_retrigger,alert.stat_id,
                                 alert.site_code,alert.alert_symbol,alert.trigger_source))
    thread_id=1560526764066319 #send to test validation 
    thread_type=ThreadType.GROUP
    
#    client.send(Message(text="testing lang :D"), thread_id=thread_id, thread_type=thread_type)
    
    client.send(Message(text=message), thread_id=thread_id, thread_type=thread_type)
    
    
    for a in os.listdir(OutputFP):
        print a
        client.sendLocalImage(OutputFP + a, message=None, thread_id=thread_id, thread_type=thread_type)
    
    client.logout()

##########################################################

query = ("SELECT stat_id, site_code,s.site_id, trigger_source, alert_symbol, "
        "ts_last_retrigger FROM "
        "(SELECT stat_id, ts_last_retrigger, site_id, trigger_source, "
        "alert_symbol FROM "
        "(SELECT stat_id, ts_last_retrigger, site_id, trigger_sym_id FROM "
        "(SELECT * FROM alert_status WHERE "
        "ts_set >= NOW()-interval 5 minute "
        "and ts_ack is NULL"
#        "stat_id=806 "
        ") AS stat "
        "INNER JOIN "
        "operational_triggers AS op "
        "ON stat.trigger_id = op.trigger_id) AS trig "
        "INNER JOIN "
        "(Select * from operational_trigger_symbols  where source_id=1) AS sym "
        "ON trig.trigger_sym_id = sym.trigger_sym_id "
        "inner join trigger_hierarchies as th "
        "on th.source_id=sym.source_id) AS alert "
        "INNER JOIN "
        "sites as s "
        "ON s.site_id = alert.site_id")
        
smsalert=qdb.get_db_dataframe(query)
#Output = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

for i in smsalert.index:
    OutputFP=main(smsalert.loc[0])
    send_messenger(OutputFP,smsalert.loc[0])