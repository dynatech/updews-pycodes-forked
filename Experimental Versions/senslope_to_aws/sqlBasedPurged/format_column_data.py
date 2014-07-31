import os,time,re,datetime
from datetime import datetime as dt
from numpy import arctan, pi, sqrt
import datetime
import ConfigParser

def printThetaFile(thetaA, thetaF):

    for i in range(0, len(thetaA)):
        data = thetaA[i].copy()

        for k in data.keys():
            val = data[k]
            thetaF.write(str(val)+',')
        thetaF.write('\n')

def printMoistureFile(mA, mF):

    for i in range(0, len(mA)):
        data = mA[i].copy()

        for k in data.keys():
            val = data[k]
            mF.write(str(val)+',')
        mF.write('\n')

# calculate the XZ tilt with this formula
#
#   XZtheta = artan( z / sqrt(x^2+y^2) )
#
def getXZvalue(entry):
    x = int(entry[3])
    y = int(entry[4])
    z = int(entry[5])
    return round(arctan(z/sqrt(x*x+y*y))*180/pi, 8)


def formatData(fname):

    print 'Reading file ' + fname + '-data.csv',

    fdir = 'C:\\Documents and Settings\\Administrator\\My Documents\\Earl\\Dropbox\\Senslope Data\\'

##    #inputfile = file('D:\\Dropbox\\senslope\\PuguisData\\'+fname + '-data.csv', 'r')
##    inputfile = file('C:\\Documents and Settings\\Administrator\\My Documents\\CindyP\\Senslope\\Server_Running\\ServerFiles\\'+fname + '-data.csv', 'r')
##    #thetafile = file('D:\\Dropbox\\senslope\\PuguisData\\'+fname + '-formatted-data.csv', 'w')
##    thetafile = file('C:\\Documents and Settings\\Administrator\\My Documents\\CindyP\\Senslope\\Server_Running\\ServerFiles\\'+fname + '-formatted-data.csv', 'w')
##    #moisturefile = file('D:\\Dropbox\\senslope\\PuguisData\\'+fname + '-moisture-data.csv', 'w')
##    moisturefile = file('C:\\Documents and Settings\\Administrator\\My Documents\\CindyP\\Senslope\\Server_Running\\ServerFiles\\'+fname + '-moisture-data.csv', 'w')

    inputfile = file(fdir+fname + '.csv', 'r')
    thetafile = file(fdir+fname + '-formatted-data.csv', 'w')
    moisturefile = file(fdir+fname + '-moisture-data.csv', 'w')

    thetaArray = []
    moistureArray = []

    all_lines = inputfile.readlines()
    all_lines.pop(0)
    inputfile.close()
    print '..done'

    # get complete column data first
    col_data = dict()
    moisture = dict()

    if fname == 'berm':
        col_data[9] = 0.089056*180/pi
        _colNodeNumbers = 14
    elif fname == 'toe':
        _colNodeNumbers = 10

   
    print 'Getting first complete column data',
    while len(col_data) < _colNodeNumbers + 1:
        entry = all_lines.pop(0).split(',')

        dt_base = float(entry[1])
        col_data[0] = entry[0] + ',' + entry[1]
        col_data[int(entry[2])] = getXZvalue(entry)
        moisture[0] = entry[0] + ',' + entry[1]
        moisture[int(entry[2])] = entry[6]
        
    print '..done'

    print 'Gathering theta information',
    
    try:
        while all_lines:
            while True:
                entry = all_lines.pop(0)
                entry = entry.split(',')
                dt_cur = float(entry[1])
                
                if float(dt_cur - dt_base) > 1.0/24.0/2.0:
                    # put all lines in thetaArray
                    thetaArray.append(col_data.copy())
                    moistureArray.append(moisture.copy())

                    # change base date and time
                    dt_base = dt_cur

                    # start new col_data 
                    col_data[0] = entry[0] + ',' + entry[1]
                    col_data[int(entry[2])] = getXZvalue(entry)
    
                    # start new moisture 
                    moisture[0] = entry[0] + ',' + entry[1]
                    moisture[int(entry[2])] = entry[6]
                    
                    break

                col_data[int(entry[2])] = getXZvalue(entry)
                moisture[int(entry[2])] = entry[6]
                
    except IndexError:
        print '..done'

    print 'Saving theta information to file',
    printThetaFile(thetaArray, thetafile)
    printMoistureFile(moistureArray, moisturefile)
    #print thetaArray[1:20]
    print '..done'

    moisturefile.close()
    thetafile.close()

def format_data():
    formatData('bolb')
    formatData('lipb')
    formatData('lipt')
    formatData('sinb')
    formatData('sint')
    formatData('sinu')
