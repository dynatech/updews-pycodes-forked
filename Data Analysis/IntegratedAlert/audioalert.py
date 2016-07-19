import subprocess
import ConfigParser
import os
import time
import signal

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

def up_one(p):
    out = os.path.abspath(os.path.join(p, '..'))
    return out

cfg = ConfigParser.ConfigParser()
cfg.read(up_one(os.path.dirname(__file__)) + '/server-config.txt')    

gsm_alert = cfg.get('I/O','gsmalert')
output_file_path = output_path + cfg.get('I/O','OutputFilePath')

if os.stat(output_file_path+gsm_alert).st_size != 0:
    cmd = 'mplayer alert.wav'
    audioalert = subprocess.Popen(cmd, shell=True)
    time.sleep(5)
    os.kill(audioalert.pid, signal.SIGINT)