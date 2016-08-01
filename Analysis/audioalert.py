import subprocess
import ConfigParser
import os
import time
import signal   

gsm_alert = 'GSMAlert.txt'

if os.stat(gsm_alert).st_size != 0:
    cmd = 'mplayer alarm.wav'
    audioalert = subprocess.Popen(cmd, shell=True)
    time.sleep(5)
    os.kill(audioalert.pid, signal.SIGINT)