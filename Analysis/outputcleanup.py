import os
from datetime import datetime, timedelta
import configfileio as cfg

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

s = cfg.config()

#local file paths
RainfallPlotsPath = output_path + s.io.rainfallplotspath 
OutputFilePath = output_path + s.io.outputfilepath

for dirpath, dirnames, filenames in os.walk(RainfallPlotsPath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 1):
            os.remove(curpath)

for dirpath, dirnames, filenames in os.walk(OutputFilePath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)
