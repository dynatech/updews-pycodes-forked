##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()

import os
from datetime import datetime, timedelta
import ConfigParser

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')    

#INPUT/OUTPUT FILES

#local file paths
OutputFilePath = output_path + cfg.get('I/O','OutputFilePath')

for dirpath, dirnames, filenames in os.walk(OutputFilePath):
    if len(filenames) == 0 and len(dirnames) == 0:
        os.rmdir(dirpath)
    for File in filenames:
        curpath = os.path.join(dirpath, File)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 1):
            os.remove(curpath)
            
for dirpath, dirnames, filenames in os.walk(OutputFilePath):
    if len(filenames) == 0 and len(dirnames) == 0:
        os.rmdir(dirpath)