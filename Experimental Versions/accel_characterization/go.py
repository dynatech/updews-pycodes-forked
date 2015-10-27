# -*- coding: utf-8 -*-

import ConfigParser
import sys

configFile = "main-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

section = "File I/O"
MachineFP = cfg.get(section,'MachineFP')
InputFP = MachineFP + cfg.get(section,'InputFP')
OutputFP = MachineFP + cfg.get(section,'OutputFP')
    
section = "Data Settings"
if (len(sys.argv)>1):
    col = sys.argv[1]
else:
    raise ValueError("No column name entered")

if sys.argv[2]=="all":
    nids = range(1,41)
else:
    nids = sys.argv[2:]

f = open("execute.bat","w")
for i in nids:
#    f.write("start python range-filter.py %s %s\n" % (col,str(i)))
    f.write("start python filtered.py %s %s\n" % (col,str(i)))

f.close()    