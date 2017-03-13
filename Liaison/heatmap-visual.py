import os
import sys

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//Analysis//Soms//'))
if not path in sys.path:
   sys.path.insert(1, path)
del path
import heatmap as htmap

site = sys.argv[1]
tdate = sys.argv[2]
days = sys.argv[3]

#site = 'mngsa'
#tdate = '2016-08-01'
#days = '1d'
data = htmap.heatmap(site, tdate , t_win = days)