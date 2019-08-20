import subprocess
import os
from datetime import datetime
from fbchat import Client
from fbchat.models import *
import pprint

class WatchDog():
    
    def main(self):
        client = Client('gelibertejohn@gmail.com', 'Decmeister12')
        threads = '2199956213379612'

        gsms = ['pgrep -f .*python.*runner.py.*-g1', 'pgrep -f .*python.*runner.py.*-g2']
        path = '/home/pi/updews-pycodes/gsm/gsmserver_dewsl3/logs'
        for proc in gsms:

            process = subprocess.Popen(proc, shell=True, stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            my_pid, err = process.communicate()
            if len(my_pid.splitlines()) > 1:
                print("Proc: "+proc+" is running")
            else:
                print("Proc: "+proc+" is not running")
                if (os.path.isfile(path+'/inactive_log.txt') != True):
                    file_open = open(path+"/inactive_log.txt","w+")

                file_append = open(path+"/inactive_log.txt", "a+")
                message = "******"+proc[-2:].upper()+" Script Inactive*******\n" \
                          "Script Status: Active\n" \
                          "Reboot ts: "+ts+"\n" \
                          "*******************************"

                if (proc[-2:] == 'g1'):
                    status = os.system("screen -dmS gsmserver-"+proc[-2:]+" /usr/bin/python3.5 /home/pi/updews-pycodes/gsm/gsmserver_dewsl3/runner.py "+proc[-3:])
                else:
                    status = os.system("screen -dmS gsmserver-"+proc[-2:]+" /usr/bin/python3.5 /home/pi/cbewsl_sync_server/runner.py "+proc[-3:])
                
                file_append.write(message+"\n\n")
                
                client.send(Message(text=message), thread_id=threads, thread_type=ThreadType.GROUP)
                file_append.close()
        client.logout()

if __name__ == "__main__":
    initialize_watchdog = WatchDog()
    initialize_watchdog.main()