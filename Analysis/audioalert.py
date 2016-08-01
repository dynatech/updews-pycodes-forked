import subprocess
import os
import time
import signal   
import querySenslopeDb as q

query = "SELECT * FROM senslopedb.smsalerts order by alert_id desc limit 100"
alertsms = q.GetDBDataFrame(query)
not_ack = alertsms.loc[alertsms.ack == 'None']

if len(not_ack) != 0:
    cmd = 'mplayer alarm.wav'
    audioalert = subprocess.Popen(cmd, shell=True)
    time.sleep(5)
    os.kill(audioalert.pid, signal.SIGINT)