"""Example of using hangups to receive chat messages.
Uses the high-level hangups API.
"""

import asyncio

import hangups

from common import run_example

import re
import os
import dynadb.db as db

import time

if __name__ == '__main__':
    query = "select link from olivia_link where link_id = 3"
    python_path = db.read(query, connection = "gsm_pi")[0][0]
    
    file_path = os.path.dirname(__file__)
    
    brain = 'UgwySAbzw-agrDF6QAB4AaABAagBp5i4CQ'
    conversation_id = brain
    message = "olivia staying alive"
    cmd = "{} {}/send_message.py --conversation-id {} --message-text '{}'".format(python_path, file_path,conversation_id,message)
    os.system(cmd)       
    time.sleep(5)
    
    q_log = ("SELECT TIMESTAMPDIFF(MINUTE, ts, NOW()) "
                 "AS difference FROM olivia_logs "
                 "where log_id = (select max(log_id) from olivia_logs)")
    ts_diff = db.read(q_log, connection= "gsm_pi")[0][0]
    
    if ts_diff>5:
        
        cmd = "pkill -f olivia_receive_message.py"
        os.system(cmd)    
        
        time.sleep(5)
        cmd = "/usr/bin/screen -dmS olivia-screen {} {}/olivia_receive_message.py".format(python_path, file_path)
        os.system(cmd)    
        