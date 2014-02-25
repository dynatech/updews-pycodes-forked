import csv
from read_update_proc_plot_data import *

class ColumnArray:
    def __init__(self, name, segment_length, number_of_segments):
        self.name = name
        self.seglen = segment_length
        self.nos = number_of_segments
        

fo = csv.reader(open('testdata.csv', 'r'),delimiter=',')
column_list = []
for line in fo:

    f = filter_good_data(int(line[0]),int(line[1]),int(line[2]))

    if f==0:
        print line
    else:
        print 'pass'

raw_input()
    
