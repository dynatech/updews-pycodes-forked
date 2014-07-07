##############################################################################
## This module reads and checks purged file and appends the processed file. ##
##############################################################################


import os
import csv
import math
import numpy as np
import scipy as sp
import scipy.optimize
import ConfigParser

from dateutil.parser import parse
from array import *
from datetime import date, timedelta
from datetime import datetime as dt
import time


class ColumnArray:
    def __init__(self, name, number_of_segments, segment_length):
        self.name = name
        self.nos = number_of_segments
        self.seglen = segment_length  


############################################################
##                   UPDATE FUNCTION                      ##
############################################################

def update_proc_file(col):
    
    inputfname=col.name+".csv"
    print "Reading",inputfname+"..."
    print "Updating",col.name+"_proc.csv..."

    ##checks if raw file exists##
    if os.path.exists(PurgedFilePath+inputfname)==0:
        print "Input file path does not exist for " + PurgedFilePath + inputfname
        print "Update failed!\n"
        return

    ##checks if processed file exists, determines the number of lines in the file##
    outputfname=col.name+"_proc.csv"
    if os.path.exists(ProcFilePath+outputfname):
        fa1 = open(ProcFilePath+outputfname,'rb') 
        fa = csv.reader(fa1,delimiter=',')
        nL=len(list(fa))
        fa1.close()

    else:
        fa1 = open(ProcFilePath+outputfname,'wb')
        fa = csv.writer(fa1,delimiter=',')
        nL=0
        fa1.close()

    ##checks, records the last date recorded in the processed files##
    if nL==0:
        last_date_proc=dt.combine(date(1999,1,1),time(0,0,0))

    else:
        fa1 = open(ProcFilePath+outputfname,'rb') 
        fa = csv.reader(fa1,delimiter=',')
        a=0

        for row in fa:
            if a==nL-1:
                strdate=row[0]
            a=a+1
        
        last_date_proc=parse(row[0])
        fa1.close()


    ##reads latest raw data and appends to the processed data file##
    fa1 = open(ProcFilePath+outputfname,'ab')    
    fa = csv.writer(fa1,delimiter=',')
    fo1 = open(PurgedFilePath+inputfname, 'rb')
    fo = csv.reader(fo1,delimiter=',')
    lines_appended=0

    for i in fo:
        try:
            if i[1]=="id":continue
        except:
            print 'IndexError detected..'
        try:
            strdate=parse(i[0])
        except:
            print 'Date parse error'
        date_check=i[0]
        Y=int(date_check[0:4])

        if Y<2009:continue
        date_raw=strdate

        if date_raw<=last_date_proc:
            continue

        else:
            dt=parse(i[0])
            nodeID=i[1]
            x=i[2]
            y=i[3]
            z=i[4]
            moi=i[5]
            if moi=='':
                moi=str(-1)

            xz,xy=accel_to_lin_xz_xy(col.seglen,int(x),int(y),int(z))
                        
            #uncomment next line below if you want to check individual sensor values and orthogonality of axes
            #tilt_data_filter=str(filter_good_data(int(x),int(y),int(z)))
            
            #uncomment next three lines below if you want to check if moisture data is valid
            #if int(moi)>=2300 and int(moi)<=4000:
            #    moifilter=str(1)
            #else:moifilter=str(0)

            #old proc format
            #row=(dt,nodeID,x,y,z,tilt_data_filter,xz,xy,moi,moifilter)

            #new proc format
            row=(dt,nodeID,xz,xy,moi)
            
            fa.writerow(row)
            lines_appended=lines_appended+1

    fa1.flush()
    fa1.close()
    fo1.close()
    print "Update successful!\n"


############################################################
##                     TILT FUNCTIONS                     ##
############################################################

def accel_to_lin_xz_xy(seg_len,xa,ya,za):

    #DESCRIPTION
    #converts accelerometer data (xa,ya,za) to corresponding tilt expressed as horizontal linear displacements values, (xz, xy)
    
    #INPUTS
    #seg_len; float; length of individual column segment
    #xa,ya,za; integers; accelerometer data (ideally, -1024 to 1024)
    
    #OUTPUTS
    #xz, xy; floats; horizontal linear displacements along the planes defined by xa-za and xa-ya, respectively; units similar to seg_len
    
    x=seg_len/np.sqrt(1+(np.tan(np.arctan(za/(np.sqrt(xa**2+ya**2))))**2+(np.tan(np.arctan(ya/(np.sqrt(xa**2+za**2))))**2)))
    xz=x*np.tan(np.arctan(za/(np.sqrt(xa**2+ya**2))))
    xy=x*np.tan(np.arctan(ya/(np.sqrt(xa**2+za**2))))
    
    return xz,xy


############################################################
##                   FILTER FUNCTION                      ##
############################################################

def filter_good_data(a1,a2,a3):

    ##DESCRIPTION
    ##checks 1) individual acclerometer value, and 2) the physical, mutual orthogonality of the accelerometer axes based on their respective values (a1,a2,a3). 
    
    ##INPUTS
    ##a1,a2,a3; integers; accelerometer data (ideally, -1023 to 1023)
    
    ##OUTPUTS
    ##filter; integer; (1) if axes are mutually orthogonal, (0) if otherwise, or at least one accelerometer value has exceeded its allowable range 
  
    
    ##defining maximum dot product value if two axes are perpendicular to each other
    threshold_dot_prod=0.05    #ATTN SENSLOPE: Please validate this value. also it might be good to move this to the config file
    
    ##internal printing options
    print_output_text=0
    
    ##setting initial value of filter
    filter=1
    
    ##ATTN SENSLOPE: This is the current filter for individual axis value. Add, edit, remove as needed. 
    ##START OF CHECKING OF INDIVIDUAL AXIS VALUE 
    temp2=(a1,a2,a3) 
    temp1=array('i')
    for ax in temp2:
       
        if ax<-1023:
            ax=ax+4096

            if ax>1223:
                filter=0
                break

            elif ax>1023:
                ax=1023
                temp1.append(ax)

            else:temp1.append(ax)

        elif ax<1024:
            temp1.append(ax)
            continue

        else:
            filter=0
            break
    ##END OF CHECKING OF INDIVIDUAL AXIS VALUE

    if filter==0:
        return filter
        
    
    
    ##START OF MUTUAL ORTHOGONALITY CHECK 
    ##arranges accel data into increasing values (due to precision issues)##
    temp_sort=np.sort(temp1)
    xa=temp_sort[0]
    ya=temp_sort[1]
    za=temp_sort[2]

    ##Assume unit sphere defined by mutually perpendicular axes i,j,k
    ##Define accelerometer axis inclinations from horizontal plane (i-j) and corresponding cones in unit sphere##
    alpha=(asin(xa/1023.0))     ##inclination from horizontal plane##
    xa_conew=1*cos(alpha)       ##cone width, measured along i^j^ space##
    xa_coneh=sin(alpha)         ##cone height, measured along k^##
    xa_k=xa_coneh
    xa_cone=sp.array([deg(alpha), xa_coneh, xa_conew])

    beta=(asin(ya/1023.0))
    ya_conew=1*cos(beta)
    ya_coneh=sin(beta)
    ya_k=ya_coneh
    ya_cone=sp.array([deg(beta), ya_coneh, ya_conew])

    gamma=(asin(za/1023.0))
    za_conew=1*cos(gamma)
    za_coneh=sin(gamma)
    za_k=za_coneh
    za_cone=sp.array([deg(gamma), za_coneh, za_conew])

    ##arbitrarily sets x-accel axis (minimum value) along plane i^k^##
    xa_i=xa_conew
    xa_j=0
    xa_k=xa_coneh
    xa_ax=sp.array([xa_i, xa_j, xa_k])  ##defines position of xa_ax##

    ##determines position of y-accel axis (intermediate value) from xa_ax and ya_coneh##
    ya_k=ya_coneh

    ##defines system of two equations##
    fya = lambda y: [(pow(y[0],2)+pow(y[1],2)+pow(ya_k,2)-1),          ##equation of cone rim##
                     (xa_ax[0]*y[0] + xa_ax[1]*y[1] + xa_ax[2]*ya_k)]  ##equation of dot product of xa and ya = 0## 

    ##solves for y[0] and y[1]##
    y0 = scipy.optimize.fsolve(fya, [0.1, 0.1])         
    ya_i=y0[0]
    ya_j=y0[1]

    ##defines 2 possible positions of ya_ax##
    ya_ax_1=sp.array([ya_i, ya_j, ya_k])    
    ya_ax_2=sp.array([ya_i, -ya_j, ya_k])       

    ##determines the appropriate ya_ax that produces a theoretical z-accel axis consistent with the sign of za##
    za_k=za_coneh
    za_ax_t=sp.cross(xa_ax,ya_ax_1)     

    if (za_ax_t[2]+1)/(1+abs(za_ax_t[2]))==(1+za_k)/(1+abs(za_k)):
        ya_ax=ya_ax_1

    else:
        ya_ax=ya_ax_2
        za_ax_t=sp.cross(xa_ax,ya_ax_2)
    
    ##determines position of z-accel axis (minimum value) from xa_ax and ya_ax using dot product function##
    ##za_ax must be perpendicular to both xa_ax and ya_ax##
    za_k=za_coneh

    ##defines system of three equations##
    gza = lambda z: [ (xa_ax[0]*z[0] + xa_ax[1]*z[1] + xa_ax[2]*za_k), ##equation of dot product of xa and za = 0##
                      (ya_ax[0]*z[0] + ya_ax[1]*z[1] + ya_ax[2]*za_k), ##equation of dot product of ya and za = 0##
                      (z[0]**2 + z[1]**2 + za_k**2 - 1)]               ##equation of cone rim##

    ##solving for z[0] and z[1]##
    z0,d,e,f= scipy.optimize.fsolve(gza, [ 0.1, 0.1, 0.1],full_output=1)   
    za_i=z0[0]  
    za_j=z0[1]  
    za_ax=sp.array([za_i,za_j,za_k])

    ##checking the dot products of xa_ax, ya_ax, za_ax##
    if abs(sp.dot(xa_ax,ya_ax))>threshold_dot_prod or abs(sp.dot(ya_ax,za_ax))>threshold_dot_prod or abs(sp.dot(za_ax,xa_ax))>threshold_dot_prod: filter=0
    if print_output_text==1:
        np.set_printoptions(precision=2,suppress=True)
        print "xa:  ",xa_ax, round(sqrt(sum(i**2 for i in xa_ax)),4)
        print "ya:  ",ya_ax, round(sqrt(sum(i**2 for i in ya_ax)),4), round(sp.dot(xa_ax,ya_ax),4) 
        print "za_t:",za_ax_t, round(sqrt(sum(i**2 for i in za_ax_t)),4), round(sp.dot(xa_ax,za_ax_t),4), round(sp.dot(ya_ax,za_ax_t),4)
        print "za:  ",za_ax, round(sqrt(sum(i**2 for i in za_ax)),4), round(sp.dot(xa_ax,za_ax),4), round(sp.dot(ya_ax,za_ax),4), round(sp.dot(za_ax_t,za_ax),4)
        print abs(sp.dot(xa_ax,ya_ax)), abs(sp.dot(ya_ax,za_ax)), abs(sp.dot(za_ax,xa_ax)), filter        
    
    ##END OF MUTUAL ORTHOGONALITY CHECK 
    
    return filter


######################################################################
##                         MAIN PROGRAM                             ##
######################################################################

##gets configuration from file##
cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')

MachineFilePath = cfg.get('File I/O','MachineFilePath')
PurgedFilePath = MachineFilePath + cfg.get('File I/O','PurgedFilePath')
ProcFilePath = MachineFilePath + cfg.get('File I/O','ProcFilePath')
ColumnPropertiesFile = cfg.get('File I/O','ColumnPropertiesFile')

try:

    fo = csv.reader(open(ColumnPropertiesFile, 'r'),delimiter=',')
    column_list = []
    for line in fo:
        col = ColumnArray(line[0], int(line[1]), float(line[2]))
        column_list.append(col)

    while True:
        for column in column_list:
            #input_file_name,num_nodes,col.name,seg_len=Input_col.name(col.name_list,num_nodes_col.name,col_seg_len_list, INPUT_which_sensor)
            #input_file_name=ProcFilePath+input_file_name

            update_proc_file(column)

        tm = dt.today()
        cur_sec = tm.minute*60 + tm.second
        interval = 30        
        sleep_tm = 0
        for i in range(0, 60*60+1, interval*60):
            if i > cur_sec:
                print i
                sleep_tm = i
                break
        print 'Sleep..',
        print sleep_tm - cur_sec
        time.sleep(sleep_tm - cur_sec)
except KeyboardInterrupt:
    print 'Keyboard interrrupt'
    raw_input("Enter anything to quit")
