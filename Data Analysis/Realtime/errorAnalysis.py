# -*- coding: utf-8 -*-
"""
This module profiles the error bands for each node, and evaluates the accumulated errors in the column position. 

The analysis involves finding modal values for a given duration of sensor data, and assumes that the modal values
at the lower and upper range represent the region where noise and actual data cannot be resolved. 
The modal values are represented by peaks in the distribution of the node values, 
approximated by a gaussian kde. Arbitrary parameters of peak height and area under the curve are used to determine 
whether a peak is signficant or not.  
"""
from scipy.stats import gaussian_kde
from scipy.interpolate import UnivariateSpline
import pandas as pd
import numpy as np


def cml_noise_profiling(xz,xy,excludenodelist):
#==============================================================================
#     description
#     determines peak/s in data distribution to characterize noise, 
#     and computes for accumulated error for the column position due to the noise
#     
#     inputs
#     xz,xy   - dataframe containing xz and xy positions
#     
#     outputs
#     xz_peaks, xy_peaks
#         - list of arrays containing the bounds of the detected noise per node
#     xz_maxlist_cml, xz_minlist_cml, xy_maxlist_cml,xy_minlist_cml 
#         - list of arrays containing cumulative maximum and minimum column positions
#==============================================================================
    
    
    #initializing maximum and minimum positions of xz and xy
    xz_maxlist=np.zeros(len(xz.columns)+1)
    xz_minlist=np.zeros(len(xz.columns)+1)
    xy_maxlist=np.zeros(len(xy.columns)+1)
    xy_minlist=np.zeros(len(xy.columns)+1)
    
    
    
    for m in xz.columns:
        n=len(xz.columns)+1-m
        
        if n in excludenodelist:
            continue
        else:    
        
            #processing XZ axis
                       
            x=xz[m].values
            x=x[np.isfinite(x)]
            try:            
                kde=gaussian_kde(x)
                xi=np.linspace(x.min()-2*(x.max()-x.min()),x.max()+2*(x.max()-x.min()),1000)
                yi=kde(xi)
                xm,ym=find_spline_maxima(xi,yi)
                
                #assigning maximum and minimum positions of xz            
                try:
                    #multimodal                
                    xz_maxlist[n]=xm.max()
                    xz_minlist[n]=xm.min()
                   
                except:
                    #unimodal
                    xz_maxlist[n]=xm
                    xz_minlist[n]=xm
                   
            except:
                #no data for current node or NaN present in current node
                try:
                    np.isfinite(x[0])==True
                    xz_maxlist[n]=x[0]
                    xz_minlist[n]=x[0]
                except:
                    xz_maxlist[n]=0
                    xz_minlist[n]=0
            
            #processing XY axis
            x=xy[m].values
            x=x[np.isfinite(x)]
            
            try:            
                kde=gaussian_kde(x)
                xi=np.linspace(x.min()-2*(x.max()-x.min()),x.max()+2*(x.max()-x.min()),1000)
                yi=kde(xi)
                xm,ym=find_spline_maxima(xi,yi)
    
                #assigning maximum and minimum positions of xy            
                try:
                    #multimodal                
                    xy_maxlist[n]=xm.max()
                    xy_minlist[n]=xm.min()
                   
                except:
                    #unimodal
                    xy_maxlist[n]=xm
                    xy_minlist[n]=xm
                
            except:
                #no data for current node or NaN present in current node
                try:
                    np.isfinite(x[0])==True
                    xy_maxlist[n]=x[0]
                    xy_minlist[n]=x[0]
                except:
                    xy_maxlist[n]=0
                    xy_minlist[n]=0
            

    xz_maxlist_cml=xz_maxlist.cumsum()
    xz_minlist_cml=xz_minlist.cumsum() 
    xy_maxlist_cml=xy_maxlist.cumsum()
    xy_minlist_cml=xy_minlist.cumsum()       
      
    return  xz_maxlist[::-1], xz_minlist[::-1],xy_maxlist[::-1], xy_minlist[::-1], xz_maxlist_cml, xz_minlist_cml, xy_maxlist_cml,xy_minlist_cml 
    
def find_spline_maxima(xi,yi,min_normpeak=0.05,min_area_k=0.05):
#==============================================================================
#     description
#     extracts peaks from the gaussian_kse function,
#     such that peaks have a minimum normalized peak height and a minimum bound area     
#     
#     inputs
#     xi,yi           points corresponding to the gaussian_kde function of the data
#     min_normpeak    minimum normalized peak of the gaussian_kde function
#     min_area_k      proportional constant multiplied to the maximum bound area to compute the minimum bound area    
#     
#     output
#     peaks[x,y]      the peak locations [x] and heights [y] of the gaussian_kde function
#==============================================================================
    
    #setting gaussian_kde points as spline    
    s0=UnivariateSpline(xi,yi,s=0)
    
    try:
        #first derivative (for gettinge extrema)        
        dy=s0(xi,1)
        s1=UnivariateSpline(xi,dy,s=0)
        
        #second derivative (for getting inflection points)
        dy2=s1(xi,1)
        s2=UnivariateSpline(xi,dy2,s=0)
        
        #solving for extrema, maxima, and inflection points
        extrema=s1.roots()
        maxima=np.sort(extrema[(s2(extrema)<0)])
        inflection=np.sort(s2.roots())
        
        try:
            #setting up dataframe for definite integrals with inflection points as bounds            
            df_integ=pd.DataFrame()
            df_integ['lb']=inflection[:-1]
            df_integ['ub']=inflection[1:]

            #assigning maxima to specific ranges
            df_integ['maxloc']=np.nan            
            for i in range(len(df_integ)):
                try:
                    len((maxima>df_integ['lb'][i])*(maxima<df_integ['ub'][i]))>0
                    df_integ['maxloc'][i]=maxima[(maxima>df_integ['lb'][i])*(maxima<df_integ['ub'][i])]
                except:
                    continue
            
            #filtering maxima based on peak height and area
            df_integ.dropna(inplace=True)
            df_integ['maxpeak']=s0(df_integ['maxloc'])
            df_integ=df_integ[df_integ['maxpeak']>0.001]
            df_integ['normpeak']=s0(df_integ['maxpeak'])/s0(df_integ['maxpeak'].values.max())
            df_integ['area']=df_integ.apply(lambda x: s0.integral(x[0],x[1]), axis = 1)
            df_integ=df_integ[(df_integ['area']>min_area_k*df_integ['area'].values.max())*(df_integ['normpeak']>min_normpeak)]
            return df_integ['maxloc'],df_integ['maxpeak']#,inflection[df_integ.index],s0(inflection[df_integ.index])

        except:
            #filtering maxima based on peak height only
            maxima=extrema[(s2(extrema)<0)*(s0(extrema)/s0(extrema).max()>min_normpeak)]
            return maxima,s0(maxima)#,inflection,s0(inflection)
    
    except:
        #unimodal kde
        return xi[np.argmax(yi)],yi.max()#,None,None