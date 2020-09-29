import subprocess
import os
from datetime import datetime
from fbchat import Client
from fbchat.models import *
import pprint
import argparse

class WatchDog():
    
    def get_arguments(self):
        parser = argparse.ArgumentParser(description="Watchdog for gsm scripts [-options]")
        parser.add_argument("-g", "--gsm_id", help="GSM ID to watch E.g. 20Users")
        try:
            args = parser.parse_args()
            return args
        except IndexError:
            print('>> Error in parsing arguments')
            error = parser.format_help()
            print(error)
            sys.exit()

    def main(self,gsm_id):
        screen = "screen -ls g"+str(gsm_id)
        status = os.system(screen)
        if (status != 0):
            os.system("screen -dmS g"+str(gsm_id)+" /usr/bin/python3.5 /home/pi/dyna3_gsm/runner.py -t "+str(gsm_id[2:])+" -db 192.168.150.253 -g"+str(gsm_id[:2]))

if __name__ == "__main__":
    initialize_watchdog = WatchDog()
    args = initialize_watchdog.get_arguments()
    initialize_watchdog.main(args.gsm_id)