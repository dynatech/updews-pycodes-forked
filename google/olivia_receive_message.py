"""Example of using hangups to receive chat messages.
Uses the high-level hangups API.
"""

import asyncio

import hangups
import os
import re
from common import run_example

import analysis.querydb as qdb
import pandas as pd
from datetime import timedelta as td
import fb.xyzrealtimeplot as xyz

import analysis.rainfall.rainfall as rain

import analysis.subsurface.plotterlib as plotter
import analysis.subsurface.proc as proc
import analysis.subsurface.rtwindow as rtw

import dynadb.db as db
import MySQLdb

def get_db_data(query):
    # Open database connection
    db = MySQLdb.connect("192.168.150.247","root","senslope","senslopedb" )
    
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    
    # execute SQL query using execute() method.
    cursor.execute(query)
    
    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
#    print ("Database data : {} ".format(data))
    
    # disconnect from server
    db.close()
    return data

def insert_db_data(query):
    # Open database connection
    db = MySQLdb.connect("192.168.150.247","root","senslope","senslopedb" )
    
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    
    # execute SQL query using execute() method.
    cursor.execute(query)
    
    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
#    print ("Database data : {} ".format(data))
    db.commit()
    # disconnect from server
    db.close()
    
    
def main(alert):    

    site = alert.site_code
    ts = alert.ts_last_retrigger
    source_id = alert.source_id
    alert_id = alert.stat_id
    
    OutputFP=  os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')) #os.path.dirname(os.path.realpath(__file__))+'/{} {}/'.format(site, ts.strftime("%Y-%m-%d %H%M"))
    OutputFP += '/node_alert_hangouts/' + '{} {} {}/'.format(alert_id, site, ts.strftime("%Y-%m-%d %H%M")) 
    OutputFP=OutputFP.replace("\\", "/")
    
    if not os.path.exists(OutputFP):
        os.makedirs(OutputFP)

    
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


async def receive_messages(client, args):
    print('loading conversation list...')
    user_list, conv_list = (
        await hangups.build_user_conversation_list(client)
    )
    conv_list.on_event.add_observer(on_event)

    print('waiting for chat messages...')
    while True:
        try:
            await asyncio.sleep(1)
        except:
            print ("error")

def on_event(conv_event):
    if isinstance(conv_event, hangups.ChatMessageEvent):
        print('received chat message: "{}"'.format(conv_event.text))
        received_msg = conv_event.text
        test_groupchat='UgwcSTTEx1yRS0DrYVN4AaABAQ'
        brain = 'UgwySAbzw-agrDF6QAB4AaABAagBp5i4CQ'
        if re.search("valid",received_msg.lower()):
            print('send')
            
#            message = "nice!"
            query = "SELECT quotations,author FROM senslopedb.olivia_quotes order by rand() limit 1"
            quote = get_db_data(query)
            message = '"{}" -{}'.format(quote[0],quote[1])
            
            conversation_id = test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
        elif re.search("olivia plot [0-9]{4}",received_msg.lower()):
            alert_id = received_msg.split(" ")[2]
            message = "wait..."
            
            conversation_id = test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
            query = ("SELECT stat_id, site_code,s.site_id, trigger_source, alert_symbol, "
                    "ts_last_retrigger,source_id FROM "
                    "(SELECT stat_id, ts_last_retrigger, site_id, trigger_source, "
                    "alert_symbol,sym.source_id FROM "
                    "(SELECT stat_id, ts_last_retrigger, site_id, trigger_sym_id FROM "
                    "(SELECT * FROM alert_status WHERE "
#                    "ts_set >= NOW()-interval 5 minute "
#                    "and ts_ack is NULL"
                    "stat_id={} "
                    ") AS stat "
                    "INNER JOIN "
                    "operational_triggers AS op "
                    "ON stat.trigger_id = op.trigger_id) AS trig "
                    "INNER JOIN "
                    "(Select * from operational_trigger_symbols  where source_id in (1,3)) AS sym "
                    "ON trig.trigger_sym_id = sym.trigger_sym_id "
                    "inner join trigger_hierarchies as th "
                    "on th.source_id=sym.source_id) AS alert "
                    "INNER JOIN "
                    "commons_db.sites as s "
                    "ON s.site_id = alert.site_id".format(alert_id))
                    
            smsalert=db.df_read(query, connection= "analysis")
            
#            for i in smsalert.index:
            try:
                OutputFP=main(smsalert.loc[0])
#                if not OutputFP:
#                    print ("nasend na!")
#                else:
                send_hangouts(OutputFP,smsalert.loc[0])
            except:
                message = "error no alert {}".format(alert_id)
            
                conversation_id = test_groupchat
                cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
                os.system(cmd)
        
        elif re.search("olivia ilan alert",received_msg.lower()):
            query = ("SELECT site_code,alert_symbol, trigger_list "
                     ",if (timestampdiff(hour, data_ts,validity)<5,'for lowering','') as stat "
                     "FROM monitoring_events "
                     "inner join commons_db.sites on sites.site_id = monitoring_events.site_id "
                     "inner join monitoring_event_alerts "
                     "on monitoring_event_alerts.event_id = monitoring_events.event_id "
                     "inner join monitoring_releases "
                     "on monitoring_event_alerts.event_alert_id = monitoring_releases.event_alert_id "
                     "inner join public_alert_symbols "
                     "on public_alert_symbols.pub_sym_id = monitoring_event_alerts.pub_sym_id "
                     "where monitoring_event_alerts.pub_sym_id >= 2 and validity > Now() and data_ts >= NOW()-INTERVAL 4 hour "
                     "order by alert_symbol desc")
            cur_alert=db.df_read(query, connection= "website")
            # remove repeating site_code
            cur_alert = cur_alert.groupby("site_code").first().reset_index()
            message = "{} alerts".format(len(cur_alert))
        
            conversation_id = test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
            if len(cur_alert)>0:
                for i in range(0,len(cur_alert)):
                    if "ND" in cur_alert.trigger_list[i]:
                        message = "{} : {} {}".format(cur_alert.site_code[i],cur_alert.trigger_list[i], cur_alert.stat[i])
                    else:
                        message = "{} : {}-{} {}".format(cur_alert.site_code[i],cur_alert.alert_symbol[i],cur_alert.trigger_list[i], cur_alert.stat[i])
                    
                    print(message)
                    conversation_id = test_groupchat
                    cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
                    os.system(cmd)
                    
        elif re.search("hi olivia",received_msg.lower()):
            query = "SELECT quotations,author FROM senslopedb.olivia_quotes order by rand() limit 1"
            quote = get_db_data(query)
            message = '"{}" -{}'.format(quote[0],quote[1])
            
            conversation_id = test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
        
        elif re.search("olivia help",received_msg.lower()):
            
            file="/home/sensordev/sdteambranch/google/olivia_help.jpg"
#            print(file)
            conversation_id = test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/upload_image.py --conversation-id {} --image '{}'".format(conversation_id,file)
            os.system(cmd)
        
        elif re.search('olivia add quote "[A-Za-z0-9.,!?() ]+" - [A-Za-z0-9.,!?() ]+',received_msg.lower()):
            quote = received_msg.split('"')
            quotation = quote[1]
            author = quote[2].replace(" - ","")
            author = author.replace("- ","")
            author = author.replace(" -","")
            author = author.replace("-","")
            
            query = "INSERT INTO `senslopedb`.`olivia_quotes` (`quotations`, `author`) VALUES ('{}', '{}');".format(quotation,author)
            quote = insert_db_data(query)
            
            
            message = '"{}" -{} --added successfully'.format(quote[1],quote[2])
            
            conversation_id = test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
if __name__ == '__main__':
    run_example(receive_messages)