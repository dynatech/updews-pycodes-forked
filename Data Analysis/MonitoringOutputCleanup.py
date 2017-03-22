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
RainfallPlotsPath = output_path + cfg.get('I/O', 'RainfallPlotsPath')
OutputFilePath = output_path + cfg.get('I/O','OutputFilePath')
GrndMeasPlotsPath = output_path + cfg.get('I/O','GrndMeasPlotsPath')


for dirpath, dirnames, filenames in os.walk(RainfallPlotsPath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 3):
            os.remove(curpath)

for dirpath, dirnames, filenames in os.walk(OutputFilePath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)
            

for dirpath, dirnames, filenames in os.walk(GrndMeasPlotsPath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)