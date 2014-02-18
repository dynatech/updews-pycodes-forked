update-proc-file.py
-requires python 2.7, scipy, numpy
-requires IO-config.txt in the same folder as pycode
-requests SENSLOPE Team for routine update of proc file by running this pycode every 30 minutes

read-proc-plot-data.py
-requires python 2.7, scipy, numpy, matplotlib and pandas
-requires IO-config.txt in the same folder as pycode
-requires update-proc-file.py
-figures are only plotted when user selects ONE specific sensor
-plt.savefig is disabled