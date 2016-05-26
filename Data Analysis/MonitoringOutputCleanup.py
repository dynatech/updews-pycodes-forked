##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()

import os
from datetime import datetime, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import ConfigParser
from collections import Counter
import csv
import fileinput
import sys
import time

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')    

#INPUT/OUTPUT FILES

#local file paths
RainfallPlotsPath = output_path + cfg.get('I/O', 'RainfallPlotsPath')
AlertAnalysisPath = output_path + cfg.get('I/O','AlertAnalysisPath')


for dirpath, dirnames, filenames in os.walk(RainfallPlotsPath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)

for dirpath, dirnames, filenames in os.walk(AlertAnalysisPath):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)