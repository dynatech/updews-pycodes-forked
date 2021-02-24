#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 10:39:27 2019

@author: brain
"""

import analysis.querydb as qdb
import pandas as pd
from datetime import timedelta as td
import os
import shutil
import fb.xyzrealtimeplot as xyz
import analysis.subsurface.filterdata as fsd
import analysis.rainfall.rainfall as rain
import analysis.surficial.markeralerts as marker

import analysis.subsurface.plotterlib as plotter
import analysis.subsurface.proc as proc
import analysis.subsurface.rtwindow as rtw
import volatile.memory as mem
import dynadb.db as db

def main(alert):    
    site_id = alert.site_id
    site = alert.site_code
    ts = alert.ts_last_retrigger
    source_id = alert.source_id
    
    OutputFP=  os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')) #os.path.dirname(os.path.realpath(__file__))+'/{} {}/'.format(site, ts.strftime("%Y-%m-%d %H%M"))
    OutputFP += '/node_alert_hangouts/' + '{} {}/'.format(site, ts.strftime("%Y-%m-%d %H%M")) 
    OutputFP=OutputFP.replace("\\", "/")
    
    if not os.path.exists(OutputFP):
        os.makedirs(OutputFP)
    else:
        return False
    
    if source_id ==1:

        
        ts_before=ts.round('4H')-td(hours=4)
        
        queryalert="""SELECT na_id,ts,t.tsm_id,tsm_name,node_id,disp_alert,vel_alert 
                    FROM node_alerts
                    inner join tsm_sensors as t
                    on t.tsm_id=node_alerts.tsm_id
                    where site_id={} and (ts between '{}' and '{}')
    
                    order by tsm_name, node_id, ts desc""".format(alert.site_id,ts_before,ts)
        dfalert=db.df_read(queryalert,connection = "analysis").groupby(['tsm_id','node_id']).first().reset_index()
        print("ok")
#        plot colpos & disp vel
        tsm_props = qdb.get_tsm_list(dfalert.tsm_name[0])[0]
        window, sc = rtw.get_window(ts)
        
        data = proc.proc_data(tsm_props, window, sc)
        plotter.main(data, tsm_props, window, sc, plot_inc=False, output_path=OutputFP)
        
        
#        plot node data
        for i in dfalert.index:
            print (dfalert.tsm_name[i],dfalert.node_id[i],dfalert.ts[i])
            
            xyz.xyzplot(dfalert.tsm_id[i],dfalert.node_id[i],dfalert.ts[i],OutputFP)
            
    elif source_id == 3:
        rain.main(site_code = site, end=ts, write_to_db = False, print_plot = True,output_path = OutputFP)
    
    elif source_id ==2:
        print("marker")
        query_alert = ("SELECT marker_id FROM marker_alerts "
                       "where ts = '{}' and alert_level >0".format(ts))
        dfalert=db.df_read(query_alert,connection = "analysis")
        
        
        for m_id in dfalert.marker_id:
            marker.generate_surficial_alert(site_id=site_id, ts = ts, marker_id=m_id)
        


        #### Open config files
        sc = mem.get('server_config')
        output_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                           '../..'))
        
        plot_path_meas = output_path+sc['fileio']['surficial_meas_path']
        plot_path_trend = output_path+sc['fileio']['surficial_trending_path']
        
        for img in os.listdir(plot_path_meas):    
            shutil.move("{}/{}".format(plot_path_meas,img), OutputFP)
        
        for img in os.listdir(plot_path_trend):    
            shutil.move("{}/{}".format(plot_path_trend,img), OutputFP)
        
    return OutputFP

def send_hangouts(OutputFP, alert):
    test_groupchat='UgwcSTTEx1yRS0DrYVN4AaABAQ'
    brain = 'UgwySAbzw-agrDF6QAB4AaABAagBp5i4CQ'
    conversation_id = test_groupchat
    
    message=("SANDBOX:\n"
            "As of {}\n"
            "Alert ID {}:\n"
            "{}:{}:{}".format(alert.ts_last_retrigger,alert.stat_id,
                                 alert.site_code,alert.alert_symbol,alert.trigger_source))

    cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
    os.system(cmd)
   
    for a in os.listdir(OutputFP):
        print (a)
        cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/upload_image.py --conversation-id {} --image '{}'".format(conversation_id,OutputFP+a)
        os.system(cmd)


##########################################################
if __name__ == '__main__':
    query = ("SELECT stat_id, site_code,s.site_id, trigger_source, alert_symbol, "
            "ts_last_retrigger,source_id FROM "
            "(SELECT stat_id, ts_last_retrigger, site_id, trigger_source, "
            "alert_symbol,sym.source_id FROM "
            "(SELECT stat_id, ts_last_retrigger, site_id, trigger_sym_id FROM "
            "(SELECT * FROM alert_status WHERE "
            "ts_set >= NOW()-interval 5 minute "
            "and ts_ack is NULL"
#            "stat_id=4071 "
            ") AS stat "
            "INNER JOIN "
            "operational_triggers AS op "
            "ON stat.trigger_id = op.trigger_id) AS trig "
            "INNER JOIN "
            "(Select * from operational_trigger_symbols  where source_id in (1,2,3)) AS sym "
            "ON trig.trigger_sym_id = sym.trigger_sym_id "
            "inner join trigger_hierarchies as th "
            "on th.source_id=sym.source_id) AS alert "
            "INNER JOIN "
            "commons_db.sites as s "
            "ON s.site_id = alert.site_id")
            
    smsalert=db.df_read(query, connection= "analysis")
    
    for i in smsalert.index:
        OutputFP=main(smsalert.loc[i])
        if not OutputFP:
            print ("nasend na!")
        else:
            send_hangouts(OutputFP,smsalert.loc[i])

        





