# -*- coding: utf-8 -*-
"""
Created on Fri Oct 15 15:07:58 2021

@author: Dynaslope
"""

import os
import pandas as pd
import dynadb.db as db

#import ops.checksent.sms as sms
#import ops.checksent.bulletin as bulletin
#import ops.checksent.raininfo as raininfo
import ops.checksent.olivianotif as notif
def ops_checker(conversation_id = "", ts = ""):
    message = ""
    query = "SELECT link from olivia_link where link_id = 3"
    python_path = db.read(query, connection = "gsm_pi")[0][0]
    
    file_path = os.path.dirname(__file__)
    
    test_groupchat='UgwcSTTEx1yRS0DrYVN4AaABAQ'
    
    if not conversation_id:
        conversation_id = test_groupchat
    if ts:
        try:
#            sms_notif = sms.main(pd.to_datetime(ts))
#            bulletin_notif = bulletin.main(pd.to_datetime(ts))
#            raininfo_notif = raininfo.main()
            ops_notif = notif.main(pd.to_datetime(ts))
        except:
            message += "**ERROR timestamp**\nMust be in format : YYYY-mm-dd HH:MM\n\n"

#            sms_notif = sms.main()
#            bulletin_notif = bulletin.main()
#            raininfo_notif = raininfo.main()
            ops_notif = notif.main()
    
    else:
#        sms_notif = sms.main()
#        bulletin_notif = bulletin.main()
#        raininfo_notif = raininfo.main()
        ops_notif = notif.main()
        
#    message += sms_notif
#    message +="\n"
#    message += bulletin_notif
#    message +="\n"
#    message += raininfo_notif
    message += ops_notif
    print(message)
    
    cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path,file_path,conversation_id,message)
    os.system(cmd)
    
if __name__ == '__main__':
    ops_checker()