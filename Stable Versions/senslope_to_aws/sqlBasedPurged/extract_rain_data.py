import datetime
import re
import ConfigParser
from numpy import *

def extract_rain():

    #fIn = file('D:\\Dropbox\\senslope\\PuguisData\\wea-data.csv', 'r')
    fIn = file('C:\\Documents and Settings\\Administrator\\My Documents\\CindyP\\Senslope\\Server_Running\\ServerFiles\\wea-data.csv', 'r')
    #fOut = file('D:\\Dropbox\\senslope\\PuguisData\\rain-formatted.csv','w')
    fOut = file('C:\\Documents and Settings\\Administrator\\My Documents\\CindyP\\Senslope\\Server_Running\\ServerFiles\\rain-formatted.csv','w')

    lines = fIn.readlines()
    lines.pop(0)
    lines.pop(0)

    sample = dict()
    sample['0'] = ''

    dt = datetime.datetime
    deltadt = datetime.timedelta
    tbase = dt.strptime('"2010-10-1 00:00:00"', '"%Y-%m-%d %H:%M:%S"')

    data = zeros((1,2))
    i = 1
    print ">> Extracting rain data..",
    while lines:
    ##for c in range(1,1000):
        s = lines.pop(0).split(',')
        
        tcur = float(s[1])

        rain = float(s[5])
        
        sum_6h = rain
        n = 1

        try:
            while tcur - data[i-n,0]<0.25:
                sum_6h = sum_6h + data[i-n,1]
                n = n + 1
        except IndexError:
            print '*'
            
        try:
            sum_1d = sum_6h
            while tcur - data[i-n,0]<1:
                sum_1d = sum_1d + data[i-n,1]
                n = n + 1
        except IndexError:
            print '*'
            
        try:
            sum_3d = sum_1d
            while tcur - data[i-n,0]<5:
                sum_3d = sum_3d + data[i-n,1]
                n = n + 1
        except IndexError:
            print '*'
            
        try:
            sum_15d = 0
            while tcur - data[i-n,0]<30:
                sum_15d = sum_15d + data[i-n,1]
                n = n + 1
        except IndexError:
            print '*'
            
        i = i+1

        l = array([tcur,rain])
        data = vstack((data,l))
    ##    print data

        fOut.write(s[0]+',')
        fOut.write(repr(tcur)+',')
        fOut.write(repr(rain)+',')
        fOut.write(repr(sum_6h)+',')
        fOut.write(repr(sum_1d)+',')
        fOut.write(repr(sum_3d)+',')
        fOut.write(repr(sum_15d)+'\n')
        #print '\n'

    print "done"

    fIn.close()
    fOut.close()
               
 
    

    

