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
from datetime import datetime, date, time, timedelta

tan=math.tan
sin=math.sin
asin=math.asin
cos=math.cos
pow=math.pow
sqrt=math.sqrt
atan=math.atan
deg=math.degrees
rad=math.radians

############################################################
##      TIME FUNCTIONS                                    ##    
############################################################

def get_rt_window(rt_window_length,roll_window_size,num_roll_window_ops,end=datetime.now()):
    
    ##DESCRIPTION:
    ##returns the time interval for real-time monitoring

    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    ##roll_window_size; integer; number of data points to cover in moving window operations
    
    ##OUTPUT: 
    ##end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

    ##set current time as endpoint of the interval
    
    ##round down current time to the nearest HH:00 or HH:30 time value
    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    #starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+((num_roll_window_ops*roll_window_size-1)/48.))
    
    return end, start, offsetstart

############################################################
##                     TILT FUNCTIONS                     ##
############################################################

def accel_to_lin_xz_xy(seg_len,xa,ya,za):

    #DESCRIPTION
    #converts accelerometer data (xa,ya,za) to corresponding tilt expressed as horizontal linear displacements values, (xz, xy)
    
    #INPUTS
    #seg_len; float; length of individual column segment
    #xa,ya,za; array of integers; accelerometer data (ideally, -1024 to 1024)
    
    #OUTPUTS
    #xz, xy; array of floats; horizontal linear displacements along the planes defined by xa-za and xa-ya, respectively; units similar to seg_len
    

    x=seg_len/np.sqrt(1+(np.tan(np.arctan(za/(np.sqrt(xa**2+ya**2))))**2+(np.tan(np.arctan(ya/(np.sqrt(xa**2+za**2))))**2)))
    xz=x*(za/(np.sqrt(xa**2+ya**2)))
    xy=x*(ya/(np.sqrt(xa**2+za**2)))
    
    return np.round(xz,4),np.round(xy,4)


def x_from_xzxy(seg_len, xz, xy):
    #DESCRIPTION
    #computes vertical displacement (x) as function of horizontal linear displacements values (xz, xy) and segment length (seg_len)
    
    #INPUTS
    #seg_len; float; length of individual column segment
    #xz,xy; arrays of floats; horizontal linear displacements values 
    
    #OUTPUT
    #x; array of floats; vertical displacements 
    
    
    cond=(xz==0)*(xy==0)
    diagbase=np.sqrt(np.power(xz,2)+np.power(xy,2))
    return np.round(np.where(cond,
                             seg_len*np.ones(len(xz)),
                             np.sqrt(seg_len**2-np.power(diagbase,2))),2)

############################################################
##      SENSOR DATA FILTER FUNCTION                      ##
############################################################

def check_good_tilt_data(a1,a2,a3):

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

############################################################
##                     OTHER FUNCTIONS                    ##
############################################################
def getmode(li):
    li.sort()
    numbers = {}
    for x in li:
        num = li.count(x)
        numbers[x] = num
    highest = max(numbers.values())
    n = []
    for m in numbers.keys():
        if numbers[m] == highest:
            n.append(m)
    return n




