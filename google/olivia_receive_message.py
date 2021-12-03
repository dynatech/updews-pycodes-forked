"""Example of using hangups to receive chat messages.
Uses the high-level hangups API.
"""

import asyncio

import hangups
import os
import re
from common import run_example
from ops_olivia import ops_checker
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

import gsm.alertmessaging as amsg
import gsm.smsparser2.smsclass as smsclass

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
    list_mes = ""
    try:
        if re.search("rain",table_name) or re.search("piezo",table_name):
            query_table = ("SELECT * FROM {} "
                           "where ts <= NOW() order by ts desc limit 1 ".format(table_name))
        else:
            query_table = ("SELECT ts, node_id, type_num FROM {} "
                           "where ts > (SELECT ts FROM {} where ts <= NOW() order by ts desc limit 1) "
                           "- interval 30 minute and ts<=NOW() ".format(table_name,table_name))
        
        last_data = db.df_read(query_table, connection= "analysis")
        latest_ts = last_data.ts.max()
        
        if dt.now()-latest_ts <= td(minutes = 30):
            list_mes += "{}: MERON ngayon\n".format(table_name)
        else:
            list_mes += "{}: WALA ngayon\n".format(table_name)        
        
        if data:
            list_mes += "latest ts: {}\n".format(latest_ts)
            if re.search("rain",table_name):
                list_mes += "rain = {}mm\n".format(last_data.rain[0])
                list_mes += "batt1 = {}\n".format(last_data.battery1[0])
                list_mes += "batt2 = {}\n".format(last_data.battery2[0])
                
            elif re.search("piezo",table_name):
                print ("piezo")
                
            else:
                #for v2 and up
                if len(table_name)>9:
                    num_nodes = last_data.groupby('type_num').size().rename('num').reset_index()
        
                    for msgid,n_nodes in zip(num_nodes.type_num,num_nodes.num):
                        list_mes += "msgid = {} ; # of nodes = {}\n".format(msgid,n_nodes)
                #for v1
                else:
                    n_nodes = last_data.node_id.count()
                    list_mes += "# of nodes = {}".format(n_nodes)
    except:
        list_mes = "error table: {}\n".format(table_name)
    
    return list_mes

def ilan_alert(link = False):
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
    message = "**{}** alerts\n".format(len(cur_alert))
    
    if len(cur_alert) == 1: 
        message = message.replace("s","")

    
    if len(cur_alert)>0:
        for i in range(0,len(cur_alert)):
            if "ND" in cur_alert.trigger_list[i]:
                message += "{} : {} {}\n".format(cur_alert.site_code[i],cur_alert.trigger_list[i], cur_alert.stat[i])
            else:
                message += "{} : {}-{} {}\n".format(cur_alert.site_code[i],cur_alert.alert_symbol[i],cur_alert.trigger_list[i], cur_alert.stat[i])
    
#    else:
    if link:
        if len(cur_alert)==0:
            magupdate = True
        else:
            magupdate = False
        
        #if am shift
        if dt.now().hour < 12:
            #check if routine
            month = dt.now().strftime("%B").lower()
            day = dt.now().isoweekday()
            query = "SELECT season_group_id FROM seasons where {} = ".format(month)
            
            if day in (2,5):
                #wet season
                print("wet season")
                query +="'w'"
            elif day ==3:
                print("dry season")
                query +="'d'"
            else:
                query+="'j'"
                
            season = db.df_read(query, connection = "common")
            
            if len(season)>0:
                routine = True
                message += "Routine Monitoring\n"
            else:
                routine = False
                
            #check if theres extended
            query = ("SELECT distinct site_code FROM monitoring_events "
                     "INNER JOIN commons_db.sites ON sites.site_id = monitoring_events.site_id "
                     "INNER JOIN monitoring_event_alerts ON monitoring_event_alerts.event_id = monitoring_events.event_id "
                     "INNER JOIN monitoring_releases ON monitoring_event_alerts.event_alert_id = monitoring_releases.event_alert_id "
                     "INNER JOIN public_alert_symbols ON public_alert_symbols.pub_sym_id = monitoring_event_alerts.pub_sym_id "
                     "WHERE alert_level = 0 and status =2 and ts_start > now() - interval 3 day "
                     "and date_format(validity,'%Y-%m-%d') + interval 1 day < now()")
            ext_site = db.read(query, connection= "website")
            
            if len(ext_site)>0:
                extended = True
                message += "Extended Monitoring **{}** sites:\n".format(len(ext_site))
                if len(ext_site) == 1: 
                    message = message.replace("sites","site")
                    
                for site in ext_site:
                    message += "{}\n".format(site[0])
            else:
                extended = False
            
            if routine or extended:
                magupdate = False
                
        if magupdate:
            query = "SELECT link from olivia_link where description = 'contacts updating'"
            contact_link = db.read(query, connection = "gsm_pi")[0][0]
            message += "Magupdate ng contacts\n{}\n".format(contact_link)
            
    print(message)

    message = message[:-1]
    return message
    
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

def send_hangouts(OutputFP, alert, conversation_id = ""):
    test_groupchat='UgwcSTTEx1yRS0DrYVN4AaABAQ'
    brain = 'UgwySAbzw-agrDF6QAB4AaABAagBp5i4CQ'
    if not conversation_id:
        conversation_id = test_groupchat
    
    message=("SANDBOX:\n"
            "As of {}\n"
            "Alert ID {}:\n"
            "{}:{}:{}".format(alert.ts_last_retrigger,alert.stat_id,
                                 alert.site_code,alert.alert_symbol,alert.trigger_source))

    cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
    os.system(cmd)
   
    for a in os.listdir(OutputFP):
        print (a)
        cmd = "{} {}/upload_image.py --conversation-id {} --image '{}'".format(python_path,file_path,conversation_id,OutputFP+a)
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

        if re.search("hi olivia",received_msg.lower()):
#            conversation_id = conv_event.conversation_id    #test_groupchat
            
            message = "Hello {}".format(user_list.get_user(conv_event.user_id).full_name)
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
            query = "SELECT quotations,author FROM senslopedb.olivia_quotes order by rand() limit 1"
            quote = get_db_data(query)
            message = '"{}" -{}'.format(quote[0],quote[1])
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
        
        elif received_msg.lower() == "invalid":
            file="{}/invalid.png".format(file_path)
            cmd = "{} {}/upload_image.py --conversation-id {} --image '{}'".format(python_path,file_path,conversation_id,file)
            os.system(cmd)        
        
        elif re.search('ack \d+ .+',received_msg.lower()):
            message = "Thanks {} for the acknowledgement".format(user_list.get_user(conv_event.user_id).full_name)
            
            email = ""
            for e in user_list.get_user(conv_event.user_id).emails:
                email += '"{}",'.format(e)
            email = email[:-1]
            ts = (conv_event.timestamp +td(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            try:
                query = ('SELECT sim_num FROM comms_db.mobile_numbers '
                         'INNER JOIN comms_db.user_mobiles '
                         'ON mobile_numbers.mobile_id = user_mobiles.mobile_id '
                         'INNER JOIN user_emails '
                         'ON user_emails.user_id = user_mobiles.user_id '
                         'WHERE email IN ({}) LIMIT 1'.format(email))
                
                sim_num = db.read(query, connection = "common")
                
                sms = smsclass.SmsInbox("",received_msg,sim_num[0][0],ts)
        
                amsg.process_ack_to_alert(sms) 
            except:
                message = "wrong formatting"
            
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)     
            
        elif re.search("\A(in)*valid",received_msg.lower()):
#            conversation_id = conv_event.conversation_id    #test_groupchat
            message = "Thanks {}".format(user_list.get_user(conv_event.user_id).full_name)
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
            query = "SELECT quotations,author FROM senslopedb.olivia_quotes order by rand() limit 1"
            quote = get_db_data(query)
            message = '"{}" -{}'.format(quote[0],quote[1])
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
        elif re.search("olivia plot [0-9]{4}",received_msg.lower()):
            alert_id = received_msg.split(" ")[2]
            message = "wait..."
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
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
                send_hangouts(OutputFP,smsalert.loc[0], conversation_id = conversation_id)
            except:
                message = "error no alert {}".format(alert_id)
            
#                conversation_id = conv_event.conversation_id    #test_groupchat
                cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
                os.system(cmd)
        
        elif re.search("olivia ilan alert",received_msg.lower()):
            message = ilan_alert()
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
                    

        
        elif re.search("olivia help",received_msg.lower()):
            
            file="{}/olivia_help.jpg".format(file_path)
#            print(file)
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/upload_image.py --conversation-id {} --image '{}'".format(python_path,file_path,conversation_id,file)
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
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
        
        elif re.search('olivia link',received_msg.lower()):
            query = "SELECT link from olivia_link where link_id = 1"
            message = db.read(query, connection = "gsm_pi")[0][0]
#            message ="https://trello.com/c/YztIYZq0/8-monitoring-operations-manual-guides-and-links"
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
        
        elif re.search('olivia manual',received_msg.lower()):
            query = "SELECT link from olivia_link where link_id = 2"
            message = db.read(query, connection = "gsm_pi")[0][0]
#            message ="https://drive.google.com/file/d/1u5cTNCkfVF--AYMaXiShOCozXE5dg7NW/view"
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
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
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
 																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																												   
        elif re.search('olivia may data',received_msg.lower()) :
            table_name = received_msg.lower().split(' ')[3]
            message = check_data(table_name, True)
            message = message[:-1]
#            for message in mes:
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
        
                        
        elif re.search('olivia check site [A-Za-z]{3}',received_msg.lower()):
            site_code = received_msg.lower().split(' ')[3]
            df_sites = mem.get("DF_SITES")
            
            message = ""
            try:
                site_id = df_sites.site_id[df_sites.site_code == site_code].values[0]
                
                query_loggers = ("SELECT * FROM (Select logger_name, model_id from commons_db.loggers "
                                 "where site_id = {} and date_deactivated is NULL and logger_id not in (141)) as l "
                                 "inner join commons_db.logger_models "
                                 "on logger_models.model_id = l.model_id".format(site_id))
                site_loggers = db.df_read(query_loggers,connection="common")
                
                for i in site_loggers.index:
                    #if has rain
                    if site_loggers.has_rain[i] == 1:
                        table_name = "rain_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        message += add_mes  
                    
                    #if has tilt
                    if site_loggers.has_tilt[i] == 1 and site_loggers.logger_type[i]!="gateway":
                        table_name = "tilt_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        message += add_mes  
                        
                    #if has soms
                    if site_loggers.has_soms[i] == 1 and site_loggers.logger_type[i]!="gateway":
                        table_name = "soms_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        message += add_mes  

                    #if has piezo
                    if site_loggers.has_piezo[i] == 1 and site_loggers.logger_type[i]!="gateway":
                        table_name = "piezo_{}".format(site_loggers.logger_name[i])
                        add_mes = check_data(table_name)
                        message += add_mes 
                message = message[:-1]
                        
            except:
                message = "error site_code: {}".format(site_code)
            
#            for message in mes:
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
        
        elif re.search('olivia server number',received_msg.lower()):
            query = ("SELECT gsm_id, gsm_name, gsm_sim_num FROM gsm_modules "
                     "where gsm_id between 2 and 7")
            server_num = db.df_read(query, resource= "sms_data")
            
            message = "Server number for MIA:\nGlobe: {}\nSmart: {}\n".format(server_num.gsm_sim_num[server_num.gsm_id ==2].values[0],server_num.gsm_sim_num[server_num.gsm_id ==3].values[0])
            message += "\nServer number for LOGGERS:\nGlobe: \n{}\n{}\nSmart: \n{}\n{}\n".format(server_num.gsm_sim_num[server_num.gsm_id ==4].values[0],server_num.gsm_sim_num[server_num.gsm_id ==6].values[0],server_num.gsm_sim_num[server_num.gsm_id ==5].values[0],server_num.gsm_sim_num[server_num.gsm_id ==7].values[0])
            
            query = ("SELECT sim_num, gsm_id FROM mobile_numbers "
                     "inner join user_mobiles "
                     "on user_mobiles.mobile_id = mobile_numbers.mobile_id "
                     "inner join commons_db.users "
                     "on users.user_id = user_mobiles.user_id "
                     "where nickname like 'CT Phone' ")
            ct_phone = db.df_read(query, resource= "sms_data")
            message += "\nCT Phone:\nGlobe: {}\nSmart: {}".format(ct_phone.sim_num[ct_phone.gsm_id ==2].values[0],ct_phone.sim_num[ct_phone.gsm_id ==3].values[0])
            
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
        elif re.search('olivia info',received_msg.lower()):
            file="{}/infographics_plot.png".format(file_path)
            cmd = "{} {}/upload_image.py --conversation-id {} --image '{}'".format(python_path,file_path,conversation_id,file)
            os.system(cmd)
            
            file="{}/infographics_invalid.png".format(file_path)
            cmd = "{} {}/upload_image.py --conversation-id {} --image '{}'".format(python_path,file_path,conversation_id,file)
            os.system(cmd)
          
        elif re.search('olivia ano number',received_msg.lower()):
            query_loggers = "select logger_name from loggers where date_deactivated is NULL"
            loggers = db.df_read(query_loggers, resource= "common_data").logger_name.to_numpy()
            
            query_users = ("SELECT nickname FROM users where nickname is not NULL "
                           "and nickname !='' and status = 1 ")
            users = db.df_read(query_users, resource= "common_data").nickname.str.lower().to_numpy()
            
            message = ""
            
            check_logger = re.findall(r" (?=("+'|'.join(loggers)+r"))", received_msg.lower())
            check_user = re.findall(r" (?=("+'|'.join(users)+r"))", received_msg.lower())
            
            if check_logger:
                for logger_name in check_logger:
                    print(logger_name)
                    query_num = ("SELECT sim_num FROM logger_mobile "
                                 "inner join commons_db.loggers "
                                 "on logger_mobile.logger_id = loggers.logger_id "
                                 "where logger_name = '{}'".format(logger_name))
                    logger_num = db.read(query_num, resource= "sms_data")[0][0]
                    print(logger_num)
                    message += "{} : {}\n".format(logger_name, logger_num)
            
            if check_user:
                for nickname in check_user:
                    print(nickname)
                    query_num = ("SELECT sim_num FROM commons_db.users "
                                 "inner join user_mobiles "
                                 "on  users.user_id = user_mobiles.user_id "
                                 "inner join mobile_numbers "
                                 "on  user_mobiles.mobile_id = mobile_numbers.mobile_id "
                                 "where nickname is not NULL "
                                 "and nickname !='' "
                                 "and users.status = 1 "
                                 "and user_mobiles.status = 1 "
                                 "and nickname like '{}%%'".format(nickname))
                    user_num = db.read(query_num, resource= "sms_data")
                    for num in user_num:    
                        print(num[0])
                        message += "{} : {}\n".format(nickname, num[0])
            
            if not check_user and not check_logger:
                print ("error!!!")
                message += "error!!!\n"
                
            message = message[:-1]
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
        elif re.search('olivia kanino',received_msg.lower()):
            numbers = re.findall(r"\d+", received_msg)
            numbers = list(map(int,numbers))
            message = ""
            for num in numbers:
                print (num)
                query_num = ("SELECT logger_name FROM logger_mobile "
                             "inner join commons_db.loggers "
                             "on logger_mobile.logger_id = loggers.logger_id "
                             "where sim_num like '%%{}'".format(num))
                try:
                    logger_name = db.read(query_num, resource= "sms_data")[0][0]
                    print(logger_name)
                    message += "{} : {}\n".format(num, logger_name)
                except IndexError:
                    print ("not a logger")
                    message += "{} : not a logger\n".format(num)
            
            message = message[:-1]
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
        elif re.search('olivia node',received_msg.lower()):
            query_loggers = "select tsm_name from tsm_sensors where date_deactivated is NULL"
            sensors = db.df_read(query_loggers, resource= "sensor_data").tsm_name.to_numpy()
            
            check_tsm = re.findall(r" (?=("+'|'.join(sensors)+r"))", received_msg.lower())
            
            message =""
            for tsm_name in check_tsm:
                try:
                    query_node = ("SELECT n_id FROM deployed_node "
                                  "inner join tsm_sensors "
                                  "on tsm_sensors.tsm_id = deployed_node.tsm_id "
                                  "where tsm_name = '{}' order by node_id".format(tsm_name))
                    nodes = db.df_read(query_node, resource= "sensor_data").n_id
                    message += "{} : ".format(tsm_name)
                    for nid in nodes:
                        message+="{},".format(nid)
                    message = message[:-1]
                    message +="\n"
                except:
                    message += "no data for {}\n".format(tsm_name)
            message = message[:-1]
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            
        elif re.search('olivia checklist',received_msg.lower()):
            query = "SELECT link from olivia_link where description = 'checklist'"
            message = db.read(query, connection = "gsm_pi")[0][0]
#            message ="https://drive.google.com/file/d/1u5cTNCkfVF--AYMaXiShOCozXE5dg7NW/view"
            
#            conversation_id = conv_event.conversation_id    #test_groupchat
            cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
            os.system(cmd)
            #            smstables.write_outbox(message=message, recipients="639176321023",
#                           gsm_id=2, table='users')
        
        elif re.search('olivia ops checker',received_msg.lower()):
            ts = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}',received_msg)[0]
#            received_msg.replace('olivia ops checker',"")
#            ts = ts.replace("'","")
#            ts = ts.replace('"',"")
#            ts = ts.replace('”',"")
#            ts = ts.replace('“',"")
            ops_checker(conversation_id, ts)
        
        ts = (conv_event.timestamp +td(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        email = ""
        for e in user_list.get_user(conv_event.user_id).emails:
            email += '"{}",'.format(e)
        email = email[:-1]
        try:
            query = 'select email_id from user_emails where email in ({}) limit 1'.format(email)
            email_id = db.read(query, connection = "common")
            
            received_msg = received_msg.replace("'","")
            received_msg = received_msg.replace('"',"")
            
            query_log = "INSERT INTO `olivia_logs` (`ts`, `conv_id`, `email_id`, `message`) VALUES ('{}', '{}', '{}', '{}');".format(ts,conversation_id,email_id[0][0],received_msg)
            db.write(query_log, connection="gsm_pi")
        except:
            print("error logging")
            
if __name__ == '__main__':
    query = "SELECT link from olivia_link where link_id = 3"
    python_path = db.read(query, connection = "gsm_pi")[0][0]
    
    file_path = os.path.dirname(__file__)
    
    run_example(receive_messages)