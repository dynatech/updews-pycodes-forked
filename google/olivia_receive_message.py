"""Example of using hangups to receive chat messages.
Uses the high-level hangups API.
"""

import asyncio

import hangups
import os
import re
from common import run_example
import shutil

import analysis.querydb as qdb
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td
import fb.xyzrealtimeplot as xyz

import analysis.rainfall.rainfall as rain
import analysis.surficial.markeralerts as marker
import analysis.subsurface.plotterlib as plotter
import analysis.subsurface.proc as proc
import analysis.subsurface.rtwindow as rtw

import volatile.memory as mem
import dynadb.db as db
import MySQLdb
import subprocess

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

def check_data(table_name = '', data = False):
    list_mes = []
    try:
        if re.search("rain",table_name):
            query_table = ("SELECT * FROM {} "
                           "where ts <= NOW() order by data_id desc limit 1 ".format(table_name))
        else:
            query_table = ("SELECT ts, node_id, type_num FROM {} "
                           "where ts > (SELECT ts FROM {} where ts <= NOW() order by ts desc limit 1) "
                           "- interval 30 minute and ts<=NOW() ".format(table_name,table_name))
        
        last_data = db.df_read(query_table, connection= "analysis")
        latest_ts = last_data.ts.max()
        
        if dt.now()-latest_ts <= td(minutes = 30):
            list_mes.append("{}: MERON ngayon".format(table_name))
        else:
            list_mes.append("{}: WALA ngayon".format(table_name))        
        
        if data:
            list_mes.append("latest ts: {}".format(latest_ts))
            if re.search("rain",table_name):
                list_mes.append("rain = {}mm".format(last_data.rain[0]))
                list_mes.append("batt1 = {}".format(last_data.battery1[0]))
                list_mes.append("batt2 = {}".format(last_data.battery2[0]))
            else:
                #for v2 and up
                if len(table_name)>9:
                    num_nodes = last_data.groupby('type_num').size().rename('num').reset_index()
        
                    for msgid,n_nodes in zip(num_nodes.type_num,num_nodes.num):
                        list_mes.append("msgid = {} ; # of nodes = {}".format(msgid,n_nodes))
                #for v1
                else:
                    n_nodes = last_data.node_id.count()
                    list_mes.append("# of nodes = {}".format(n_nodes))
    except:
        list_mes = ["error table: {}".format(table_name)]
    
    return list_mes
    
def main(alert):    
    site_id = alert.site_id
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
    
    elif source_id ==2:
#        print("marker")
#        query_alert = ("SELECT marker_id FROM marker_alerts "
#                       "where ts = '{}' and alert_level >0".format(ts))
#        dfalert=db.df_read(query_alert,connection = "analysis")
        
        
#        for m_id in dfalert.marker_id:
        marker.generate_surficial_alert(site_id=site_id, ts = ts)#, marker_id=m_id)
        


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


async def receive_messages(client, args):
    global user_list
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
    global user_list
    if isinstance(conv_event, hangups.ChatMessageEvent):
        print('received chat message: "{}"'.format(conv_event.text))
        received_msg = conv_event.text
        test_groupchat='UgwcSTTEx1yRS0DrYVN4AaABAQ'
        brain = 'UgwySAbzw-agrDF6QAB4AaABAagBp5i4CQ'
        
        conversation_id = conv_event.conversation_id    #test_groupchat
        
        if re.search("valid",received_msg.lower()):
#            conversation_id = conv_event.conversation_id    #test_groupchat
            message = "Thanks {}".format(user_list.get_user(conv_event.user_id).full_name)
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
            query = "SELECT quotations,author FROM senslopedb.olivia_quotes order by rand() limit 1"
            quote = get_db_data(query)
            message = '"{}" -{}'.format(quote[0],quote[1])
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
        elif re.search("olivia plot [0-9]{4}",received_msg.lower()):
            alert_id = received_msg.split(" ")[2]
            message = "wait..."
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
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
                    "(Select * from operational_trigger_symbols  where source_id in (1,2,3)) AS sym "
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
            
#                conversation_id = conv_event.conversation_id    #test_groupchat
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
        
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
            if len(cur_alert)>0:
                for i in range(0,len(cur_alert)):
                    if "ND" in cur_alert.trigger_list[i]:
                        message = "{} : {} {}".format(cur_alert.site_code[i],cur_alert.trigger_list[i], cur_alert.stat[i])
                    else:
                        message = "{} : {}-{} {}".format(cur_alert.site_code[i],cur_alert.alert_symbol[i],cur_alert.trigger_list[i], cur_alert.stat[i])
                    
                    print(message)
#                    conversation_id = conv_event.conversation_id    #test_groupchat
                    cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
                    os.system(cmd)
                    
        elif re.search("hi olivia",received_msg.lower()):
#            conversation_id = conv_event.conversation_id    #test_groupchat
            
            message = "Hello {}".format(user_list.get_user(conv_event.user_id).full_name)
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
            query = "SELECT quotations,author FROM senslopedb.olivia_quotes order by rand() limit 1"
            quote = get_db_data(query)
            message = '"{}" -{}'.format(quote[0],quote[1])
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
        
        elif re.search("olivia help",received_msg.lower()):
            
            file="/home/sensordev/sdteambranch/google/olivia_help.jpg"
#            print(file)
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/upload_image.py --conversation-id {} --image '{}'".format(conversation_id,file)
            os.system(cmd)
        
        elif (re.search("""olivia add quote "[A-Za-z0-9.,!?()'’ ]+"[-A-Za-z0-9.,!?() ]+""",received_msg.lower()) or
             re.search("""olivia add quote “[A-Za-z0-9.,!?()'’ ]+”[-A-Za-z0-9.,!?() ]+""",received_msg.lower()) ):
            
            received_msg = received_msg.replace('“','"')
            received_msg = received_msg.replace('”','"')
            received_msg = received_msg.replace("’","'")
            
            quote = received_msg.split('"')
            quotation = quote[1].replace("'","")
            quotation = quotation.replace('"',"")
            
            author = quote[2].replace(" - ","")
            author = author.replace("- ","")
            author = author.replace(" -","")
            author = author.replace("-","")
            
            query = "INSERT INTO `senslopedb`.`olivia_quotes` (`quotations`, `author`) VALUES ('{}', '{}');".format(quotation,author)
            insert_db_data(query)
            
            
            message = '"{}" -{} --added successfully'.format(quotation, author)
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
        
        elif re.search('olivia link',received_msg.lower()):
            message ="https://trello.com/c/YztIYZq0/8-monitoring-operations-manual-guides-and-links"
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message_link.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
        
        elif re.search('olivia manual',received_msg.lower()):
            message ="https://drive.google.com/file/d/1u5cTNCkfVF--AYMaXiShOCozXE5dg7NW/view"
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message_link.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
            
        elif re.search('olivia ping',received_msg.lower()):
            try:
                ipadd = received_msg.split(" ")[2]
                
                result = os.system("ping -c 1 {}".format(ipadd))
                if result == 0:
                    ping_response = subprocess.Popen(["ping", ipadd, "-c", '1'], stdout=subprocess.PIPE).stdout.read().decode("utf-8")
                    if (re.search('unreachable',ping_response)):
                        message = "Unreachable network in {}".format(ipadd)
                    else:
                        message = "Ok network in {}".format(ipadd)
                else:
                    message = "NOT ok network in {}".format(ipadd)
            except:
                message = "error input"
            cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
            os.system(cmd)
 																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																												   
        elif re.search('olivia may data',received_msg.lower()) :
            table_name = received_msg.lower().split(' ')[3]
            mes = check_data(table_name, True)
            
            for message in mes:
                cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
                os.system(cmd)
        
                        
        elif re.search('olivia check site [A-Za-z]{3}',received_msg.lower()):
            site_code = received_msg.lower().split(' ')[3]
            df_sites = mem.get("DF_SITES")
            
            mes = []
            try:
                site_id = df_sites.site_id[df_sites.site_code == site_code].values[0]
                
                query_loggers = ("SELECT * FROM (Select logger_name, model_id from commons_db.loggers "
                                 "where site_id = {} and date_deactivated is NULL and logger_id not in (141)) as l "
                                 "inner join commons_db.logger_models "
                                 "on logger_models.model_id = l.model_id".format(site_id))
                site_loggers = db.df_read(query_loggers,connection="common")
                mes = []
                for i in site_loggers.index:
                    #if has rain
                    if site_loggers.has_rain[i] == 1:
                        table_name = "rain_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        mes.extend(add_mes)  
                    
                    #if has tilt
                    if site_loggers.has_tilt[i] == 1 and site_loggers.logger_type[i]!="gateway":
                        table_name = "tilt_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        mes.extend(add_mes)  
                        
                    #if has soms
                    if site_loggers.has_soms[i] == 1 and site_loggers.logger_type[i]!="gateway":
                        table_name = "soms_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        mes.extend(add_mes)  
                        
            except:
                mes = ["error site_code: {}".format(site_code)]
            
            for message in mes:
                cmd = "/home/sensordev/miniconda3/bin/python3.7 ~/sdteambranch/google/send_message.py --conversation-id {} --message-text '{}'".format(conversation_id,message)
                os.system(cmd)

if __name__ == '__main__':
    run_example(receive_messages)