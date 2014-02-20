######################################################################################################################################################################################################################
## This program provides a spline-fit of data points representing individual node tilts. A time-series plot of raw data points and spline-fit curve, and the velocity (derivative of spline-fit curve) is produced. ## 
## The program also makes a cartesian plot of column segment positions along the xg-zg and xg-yg planes. Two column segment positions plots are produced: one showing absolute column position, and another showing ##
## positions relative to initial date of data range provided by user. The end of the bottom segment is assumed to be fixed to stable ground, and thus, positions are measured from this point.                      ##
######################################################################################################################################################################################################################


import os
import csv
import math
import numpy as np
import scipy as sp
import scipy.optimize
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import scipy.stats.stats as st
import ConfigParser
import sys

from array import *
from datetime import datetime, date, time, timedelta
from matplotlib.ticker import MaxNLocator, AutoMinorLocator
from matplotlib import font_manager as font
from scipy.interpolate import UnivariateSpline


############################################################
##                      CONSTANTS                         ##
############################################################

##############
# INPUT DATA #
##############
loc_col_list=("eeet","sinb_purged","sint_purged","sinu_purged","lipb_purged","lipt_purged","bolb_purged","pugb_purged","pugt_purged","mamb_purged","mamt_purged","oslt_purged","oslb_purged","labt_purged", "labb_purged", "gamt_purged","gamb_purged", "humt_purged","humb_purged", "plat_purged","plab_purged","blct_purged","blcb_purged")
num_nodes_loc_col=(14,29,19,29,28,31,30,14,10,29,24,21,23,39,25,18,22,21,26,39,40,24,19)
col_seg_len_list=(0.5,1,1,1,0.5,0.5,0.5,1.2,1.2,1.0,1.0,1.,1.,1.,1.,1.,1.,1.,1,0.5,0.5,1,1)
invalid=("eeet","sinb_purged","sint_purged","sinu_purged","lipb_purged","lipt_purged","bolb_purged","pugb_purged","pugt_purged",[26],"mamt_purged","oslt_purged","oslb_purged","labt_purged", "labb_purged", "gamt_purged","gamb_purged", "humt_purged","humb_purged", "plat_purged","plab_purged","blct_purged","blcb_purged")

##################
#THRESHOLD VALUES#
##################
thresholdtilt=0.05 #degrees
offsettilt=3
thresholdvel=0.005 #degrees/day
offsetvel=3

###################
#LEGEND PARAMETERS#
###################
legend_font_props = font.FontProperties()
legend_font_props.set_size('x-small')
ncol=1
bbtoa=(1,0.5)
loc="center left"
prop=legend_font_props

###################
# GRID PARAMETERS #
###################
g_which='both'
g_ax='both'
g_ls='-'
g_c='0.6'


########################
# MATH/TRIGO FUNCTIONS #
########################
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
##                       LOGGER                           ##
############################################################

class Logger(object):
    def __init__(self, filename="Default.log"):
        self.terminal = sys.stdout
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        

############################################################
##                  INPUT FILE READER                     ##
############################################################

def Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list,IWS):
    loc_col=loc_col_list[IWS]
    num_nodes=num_nodes_loc_col[IWS]
    fname=loc_col+"_proc.csv"
    seg_len=col_seg_len_list[IWS]

    return fname, num_nodes,loc_col,seg_len


############################################################
##                ARRAY CREATOR & WRITER                  ##
############################################################

def Create_Arrays_for_Input(num_nodes):
    all_nodes_data=range(num_nodes)

    return all_nodes_data

def Write_Input_File_to_Arrays(all_nodes_data,fname,node_len):
    num_nodes=len(all_nodes_data)
    cur_date=datetime.now()
    last_good_tilt_date_all_nodes=datetime.combine(date(1999,1,1),time(0,0,0))
    first_good_tilt_date_all_nodes=datetime.combine(date(2999,1,1),time(0,0,0))

    if os.path.exists(fname)==0:
        print "Error reading input file to arrays: Input file " + fname + " does not exist"
        return []        
    
    for cur_node_ID in range(num_nodes):
        dt=[]
        xz=array('f')
        xy=array('f')
        #moi=array('i') ##for moisture implementation only##
        
        fo = csv.reader(open(fname, 'r'),delimiter=',')
        total_data_count=0

        for i in fo:
            if int(i[1])==cur_node_ID+1:
                tilt_data_filter=int(i[5])

                if tilt_data_filter==1:
                    xz.append(float(i[6]))
                    xy.append(float(i[7]))
                    #moi.append(int(i[10])) ##for moisture implementation only##

                    temp=i[0]
                    Y,m,d,H,M,S= int(temp[0:4]), int(temp[5:7]), int(temp[8:10]), int(temp[11:13]), int(temp[14:16]), int(temp[17:19])
                    cur_node_dt=datetime.combine(date(Y,m,d),time(H,M,S))
                    dt.append(cur_node_dt)
                    
                    total_data_count=total_data_count+1

        xlin,xzlin,xylin=xzxy_to_cart(node_len, xz, xy)

        cur_node_data=(dt,xzlin,xylin,xlin) 
        all_nodes_data[cur_node_ID]=cur_node_data

    return all_nodes_data


############################################################
##                   "FOR PLOTTING" FUNCTIONS             ##
############################################################

def compute_colpos_time(end_dt,days,numcolpos):
    colposdays=np.linspace(-days,0,numcolpos)
    colposdates=day_to_date(colposdays, end_dt)

    return colposdates

def compute_plot_intervals(last,first,plot_intervals):
    column_data_range=last-first
    column_data_range_td=(column_data_range.days)+(column_data_range.seconds/(3600*24.0))
    dt_int=range(plot_intervals+1)

    for x in range(plot_intervals+1):
        dt_int[x]=first+(x*timedelta(days=(column_data_range_td/(1.0*(plot_intervals)))))
        if x!=0:
            return dt_int

def accumulate_translate(X,XZ,XY,numnodes,numcolpos, dates_to_plot, loc_col):
    plt.figure(20)
    plt.clf()
    plt.figure(21)
    plt.clf()
    ac_x=np.ndarray(shape=(numcolpos,numnodes+1))
    ac_xz=np.ndarray(shape=(numcolpos,numnodes+1))
    ac_xy=np.ndarray(shape=(numcolpos,numnodes+1))

    for col in range(numcolpos):
        sum_x=0
        sum_xz=0
        sum_xy=0
        ac_x[col,0]=sum_x
        ac_xz[col,0]=sum_xz
        ac_xy[col,0]=sum_xy

        for row in range(numnodes):
            sum_x=sum_x+X[numnodes-row-1,col]
            sum_xz=sum_xz+XZ[numnodes-row-1,col]
            sum_xy=sum_xy+XY[numnodes-row-1,col]
            ac_x[col,row+1]=sum_x
            ac_xz[col,row+1]=sum_xz
            ac_xy[col,row+1]=sum_xy
    
        posdate=dates_to_plot[col].strftime("%Y-%m-%d")

        for g in [1,2]:
            colposfig=plt.figure(20+g-1)
            xz_ax=colposfig.add_subplot(121)
            xy_ax=colposfig.add_subplot(122, sharey=xz_ax, sharex=xz_ax)

            if g==1:
                ref_ac_xz=np.zeros(len(ac_xz[0]))
                ref_ac_xy=np.zeros(len(ac_xy[0]))

                if col==0:
                    colposfig.suptitle(loc_col+"absolute column positions")

            else:
                ref_ac_xz=ac_xz[0]
                ref_ac_xy=ac_xy[0]

                if col==0:
                    colposfig.suptitle(loc_col+" column positions relative to "+dates_to_plot[col].strftime("%Y-%m-%d"))

            plt.sca(xz_ax)
            xzline,=xz_ax.plot(ac_xz[col]-ref_ac_xz,ac_x[col],'o-', markersize=3, label=posdate)
            curcolor=plt.getp(xzline,'color')
            xz_ax.set_ylabel("vertical position, m")
            xz_ax.set_xlabel("zg, m;\n(+) towards downslope")
            xz_ax.set_ylabel("xg, m;\n(+) towards the surface",horizontalalignment='center')
            xz_ax.set_title("xg-zg-plane")

            if g==2:
                plt.xlim(-0.15,0.15)
                xz_ax.axvline(x=-.10,color='r', ls='-',lw=1,)
                xz_ax.axvline(x=.10,color='r', ls='-',lw=1,)
                xz_ax.axvline(x=.05,color='y', ls='-',lw=1,)
                xz_ax.axvline(x=-.05,color='y', ls='-',lw=1,)
            xz_ax.yaxis.set_major_locator(MaxNLocator(10))
            xz_ax.yaxis.set_minor_locator(AutoMinorLocator(2))
            xz_ax.xaxis.set_major_locator(MaxNLocator(7))
            xz_ax.grid(b=None, which=g_which,axis=g_ax,ls=g_ls,c=g_c)
            

            plt.sca(xy_ax)
            xyline,=xy_ax.plot(ac_xy[col]-ref_ac_xy,ac_x[col],'o-', color=curcolor, markersize=3, label=posdate)
            xy_ax.set_xlabel("yg, m;\n(+) to the right, facing downslope")
            xy_ax.set_title("xg-yg-plane")

            if g==2:
                plt.xlim(-0.15,0.15)
                xy_ax.axvline(x=-.10,color='r', ls='-',lw=1,)
                xy_ax.axvline(x=.10,color='r', ls='-',lw=1,)
                xy_ax.axvline(x=.05,color='y', ls='-',lw=1,)
                xy_ax.axvline(x=-.05,color='y', ls='-',lw=1,)

            xy_ax.yaxis.set_major_locator(MaxNLocator(10))
            xy_ax.yaxis.set_minor_locator(AutoMinorLocator(2))
            xy_ax.xaxis.set_major_locator(MaxNLocator(7))
            xy_ax.grid(b=None, which=g_which,axis=g_ax,ls=g_ls,c=g_c) 
            
            xy_ax.legend(loc='upper center',bbox_to_anchor=(0.7, 1.2),prop=legend_font_props)

            plt.tight_layout()
            plt.subplots_adjust(top=0.85)

    plt.figure(20)
    fig_name=OutputFigurePath+loc_col+"_colpos_abs.png"
    if PrintFigures:
        plt.savefig(fig_name, dpi=100, facecolor='w', edgecolor='w',orientation='landscape')

    plt.figure(21)
    fig_name=OutputFigurePath+loc_col+"_colpos_rel.png"
    if PrintFigures:
        plt.savefig(fig_name, dpi=100, facecolor='w', edgecolor='w',orientation='landscape')   
        
    return ac_x, ac_xz, ac_xy


############################################################
##               UNIT CONVERSION FUNCTIONS                ##
############################################################

def xzxy_to_cart(node_len, xz, xy):
    ##xz and xy in angular units (degrees)
    cond=(xz==0)*(xy==0)

    H=np.round(np.where(cond,
                        node_len*np.ones(len(xz)),
                        node_len/np.sqrt(1+(np.tan(np.radians(xz)))**2+(np.tan(np.radians(xy)))**2)),2)

    a=np.where(cond,
               np.zeros(len(xz)),
               (np.round(H*np.tan(np.radians(xz)),4)))

    b=np.where(cond,
               np.zeros(len(xz)),
               (np.round(H*np.tan(np.radians(xy)),4)))
        
    return H,a,b

def xzxy_to_cart2(node_len, xz, xy):
    ##xz and xy already in linear units
    cond=(xz==0)*(xy==0)
    diagbase=np.sqrt(np.power(xz,2)+np.power(xy,2))

    H=np.round(np.where(cond,
                        node_len*np.ones(len(xz)),
                        np.sqrt(node_len**2-np.power(diagbase,2))),2)
    a=xz
    b=xy

    return H,a,b


############################################################
##                SPLINE-FITTING FUNCTIONS                ##
############################################################

def extract_tilt_for_colpos(splinef, colposdates, minspline, maxspline):
    tilt=array('f')
    end_date=max(colposdates)
    colposdays=[date_to_day(curdate, end_date) for curdate in colposdates]
    
    for c in range(len(colposdays)):
        if colposdays[c]<minspline:
            tilt.append(splinef(minspline))            

        elif colposdays[c]>maxspline:
            tilt.append(splinef(maxspline))

        else:
            tilt.append(splinef(colposdays[c]))

    return tilt

def fitspline_tilt(date, tilt, days, date_end):
    if len(tilt)<10:
        return [],[],[],[],[],[]

    date_start=date_end-timedelta(days=days)
    subdays=array('f')
    subtilt=array('f')
    d=0

    while d<=len(date)-1:
        if date[d]>date_end:break
        days_before=date_end-date[d]
        days_before=0-(days_before.days+(days_before.seconds/(60*60*24.)))

        if days_before<-days:
            d=d+1     
            continue

        elif days_before>0:break

        else:
            subdays.append(days_before)
            subtilt.append(tilt[d])
            d=d+1

    if len(subtilt)<10:
        return [],[],[], [], [], [] 
    
    subtilt=np.asarray(subtilt)
   
    s0 = UnivariateSpline(subdays, subtilt, s=0)
    d2s0=abs(s0(subdays,2))
    weight=[100-st.percentileofscore(d2s0, score, kind='rank') for score in d2s0]
    sf=round(np.var(d2s0)/100.,1)
    
    s = UnivariateSpline(subdays, subtilt, w=weight, s=sf)
    
    if np.sum([a for a in np.isnan(s(subdays))])>0:
        s = UnivariateSpline(subdays, subtilt, w=weight)

    if days<=30:
        subdays_fine = np.linspace(min(subdays), max(subdays), (max(subdays)-min(subdays))*50)

    elif days<=90:
        subdays_fine = np.linspace(min(subdays), max(subdays), (max(subdays)-min(subdays))*3)

    else:
        subdays_fine = np.linspace(min(subdays), max(subdays), 100)

    subtilt_fine = s(subdays_fine)
    subdtilt_fine= s(subdays_fine,1)

    return subtilt, subdays, subtilt_fine, subdtilt_fine, subdays_fine, s

    
############################################################
##                    TIME FUNCTIONS                      ##
############################################################

def day_to_date(days, end_date):
    date=[end_date+timedelta(days=x) for x in days]

    return date

def date_to_day(curdate, end_date):
    daysbefore=end_date-curdate
    daysbefore=(daysbefore.days+(daysbefore.seconds/(60*60*24.)))

    return -1*daysbefore


############################################################
##                PLOT-GENERATING FUNCTION                ##
############################################################

def GeneratePlots():

    ####################
    #SENSOR COLUMN LIST#
    ####################
    loc_col_list=("eeet","sinb_purged","sint_purged","sinu_purged","lipb_purged","lipt_purged","bolb_purged","pugb_purged","pugt_purged","mamb_purged","mamt_purged","oslt_purged","oslb_purged","labt_purged", "labb_purged", "gamt_purged","gamb_purged", "humt_purged","humb_purged", "plat_purged","plab_purged","blct_purged","blcb_purged")
    loc_col_name=("eeet","sinb","sint","sinu","lipb","lipt","bolb","pugb","pugt","mamb","mamt","oslt","oslb","labt", "labb", "gamt","gamb", "humt","humb", "plat","plab","blct","blcb")
    num_nodes_loc_col=(14,29,19,29,28,31,30,14,10,29,24,21,23,39,25,18,22,21,26,39,40,24,19)
    col_seg_len_list=(0.5,1,1,1,0.5,0.5,0.5,1.2,1.2,1.0,1.0,1.,1.,1.,1.,1.,1.,1.,1,0.5,0.5,1,1)

    check_another_sensor=1
    while check_another_sensor==1:
        print "all:      -1"

        for hj in range(len(loc_col_name)):
            print loc_col_name[hj],":     ",hj

        which_sensor=int(raw_input("Input which sensor (number) to plot: "))

        cur_date=datetime.now()

        csvout=[]
        for INPUT_which_sensor in range(len(loc_col_list)):

            if which_sensor!=-1:
                if which_sensor!=INPUT_which_sensor:continue
            
            ##defines input file name, number of nodes, name and segment lengths of columns##
            input_file_name,num_nodes,loc_col,seg_len=Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list, INPUT_which_sensor)
            input_file_name=OutputFilePath+input_file_name

            ##creates array for whole data set##
            all_nodes_data1=Create_Arrays_for_Input(num_nodes)

            ##reads text file into file objects and assigns data from file object into arrays
            all_nodes_data=Write_Input_File_to_Arrays(all_nodes_data1,input_file_name,seg_len)
            if all_nodes_data==[]:continue

            ##defines data range to be plotted##
            latest_record_time=datetime.combine(date(1999,1,1),time(0,0,0))
            if which_sensor==-1:manual_end_date_input=2
            else:manual_end_date_input=int(raw_input("Manually input end date? (1)Yes     (2)No: "))

            if manual_end_date_input==1:
                Y=int(raw_input("     Input end year: "))
                m=int(raw_input("     Input end month: "))
                d=int(raw_input("     Input end date: "))
                H=int(raw_input("     Input end hour: "))
                end_dt=datetime.combine(date(Y,m,d),time(H,0,0))
                now_time=end_dt

            else:
                now_time=datetime.now()

            print "\nDATA for ",loc_col_name[INPUT_which_sensor]," as of ", now_time.strftime("%Y-%m-%d %H:%M")    

            INPUT_fit_interval=3 ##number of days to be plotted##
            INPUT_days_to_plot=INPUT_fit_interval
            start_dt=now_time-timedelta(days=INPUT_fit_interval)
            INPUT_number_colpos=INPUT_fit_interval+1
            dates_to_plot=compute_colpos_time(now_time,INPUT_fit_interval,INPUT_number_colpos)
            compute_plot_intervals(now_time,start_dt,INPUT_number_colpos)

            ##extracts data within defined date range##
            allnodes_splinefit=range(num_nodes)    

            allnodes_colpos_splinefit_X=np.ndarray(shape=(num_nodes,INPUT_number_colpos))
            allnodes_colpos_splinefit_XZ=np.ndarray(shape=(num_nodes,INPUT_number_colpos))
            allnodes_colpos_splinefit_XY=np.ndarray(shape=(num_nodes,INPUT_number_colpos))

            colstatus=np.ndarray(shape=(num_nodes,6),dtype=int)

            for INPUT_which_node in range(num_nodes):
                colstatus[INPUT_which_node,0]=INPUT_which_node+1
                cur_node_data=all_nodes_data[INPUT_which_node]
                cur_node_date=cur_node_data[0]
                cur_node_tilt_xz=cur_node_data[1]
                cur_node_tilt_xy=cur_node_data[2]

                colstatus[INPUT_which_node,1]=1

                xzsplinefit=[[],[],[],[],[],[]]
                xysplinefit=[[],[],[],[],[],[]]
                curnode_splinefit=[xzsplinefit, xysplinefit]
                allnodes_splinefit[INPUT_which_node]=curnode_splinefit

                if len(cur_node_date)==0:
                    colpos_xztilt=np.zeros(INPUT_number_colpos)
                    colpos_xytilt=np.zeros(INPUT_number_colpos)

                else:
                    colpos_xztilt=np.asarray([cur_node_tilt_xz[-1] *(g/g) for g in range(1,INPUT_number_colpos+1)])
                    colpos_xytilt=np.asarray([cur_node_tilt_xy[-1] *(h/h) for h in range(1,INPUT_number_colpos+1)])

                    if cur_node_date[-1]>=now_time-timedelta(days=1.):
                        ##computes spline fit of xz tilt of current node within defined date range##
                        subtilt, subdays, subtilt_fine, subdtilt_fine, subdays_fine,spline = fitspline_tilt(cur_node_date,cur_node_tilt_xz,INPUT_days_to_plot,now_time)     
                        xzsplinefit=[subtilt, subdays, subtilt_fine, subdtilt_fine, subdays_fine,spline]

                        if subdays<-1 or len(subdays)==0:
                            ##sub dbase not updated##
                            py=0

                        else:
                            if np.sum([a for a in np.isnan(subtilt)])>0 or np.sum([b for b in np.isnan(subtilt_fine)])>0:
                                ##dbase updated, spline fit failed##
                                py=0

                            else:
                                colpos_xztilt=extract_tilt_for_colpos(spline, dates_to_plot, min(subdays), max(subdays))
                                                            
                                ##computes spline fit of xy tilt of current node within defined date range##
                                subtilt, subdays, subtilt_fine, subdtilt_fine, subdays_fine,spline = fitspline_tilt(cur_node_date,cur_node_tilt_xy,INPUT_days_to_plot,now_time)     
                                xysplinefit=[subtilt, subdays, subtilt_fine, subdtilt_fine, subdays_fine,spline]
                            
                                if subdays<-1 or len(subdays)==0:
                                    ##sub dbase not updated##
                                    py=0

                                else:
                                    if np.sum([a for a in np.isnan(subtilt)])>0 or np.sum([b for b in np.isnan(subtilt_fine)])>0:
                                        ##dbase updated, spline fit failed##
                                        py=0

                                    else:
                                        ##dbase updated, spline fit successful##
                                        colpos_xytilt=extract_tilt_for_colpos(spline, dates_to_plot, min(subdays), max(subdays))
                                        colstatus[INPUT_which_node,1]=0  
                                        curnode_splinefit=[xzsplinefit, xysplinefit]
                                        allnodes_splinefit[INPUT_which_node]=curnode_splinefit
                           
                X,XZ,XY=xzxy_to_cart2(seg_len, colpos_xztilt, colpos_xytilt)
                for q in range(INPUT_number_colpos):
                    allnodes_colpos_splinefit_X[INPUT_which_node,q]=round(X[q],2)
                    allnodes_colpos_splinefit_XZ[INPUT_which_node,q]=round(XZ[q],4)
                    allnodes_colpos_splinefit_XY[INPUT_which_node,q]=round(XY[q],4)
                
      #**********************************#
      #*TILT & VELOCITY THRESHOLD VALUES*#
      #**********************************#

            printfigures=0
            Tvela1=0.005 #m/day
            Tvela2=0.5 #m/day
            Ttilt=0.05 #m
            op_axis_k=0.1
            adj_node_k=0.5

            for cur_node in range(num_nodes):
                nodealert=-1
                curnode_splinefit=allnodes_splinefit[cur_node]
                xzsplinefit=curnode_splinefit[0]
                xysplinefit=curnode_splinefit[1]
                xzdays=xzsplinefit[4]
                xztilt=xzsplinefit[2]
                
                if len(xztilt)==0:
                    out=[cur_node+1,-1]
                    print out
                    csvout.append([now_time.strftime("%Y-%m-%d %H:%M"),loc_col_name[INPUT_which_sensor],
                                    str(cur_node+1),
                                    str(nodealert)])
                    continue

                xzvel=xzsplinefit[3]
                xytilt=xysplinefit[2]
                xyvel=xysplinefit[3]

                if abs(xzvel[-1])>=abs(xyvel[-1]):
                    if abs(xzvel[-1])>Tvela1:
                        if abs(xztilt[-1]-xztilt[0])<=Ttilt:
                            nodealert=0
                        else:
                            if abs(xzvel[-1])<=Tvela2:
                                if abs(xyvel[-1])>op_axis_k*abs(xzvel[-1]):
                                    nodealert=1
                                else:
                                    nodealert=0
                            else:
                                if abs(xyvel[-1])>op_axis_k*abs(xzvel[-1]):
                                    nodealert=2
                                else:
                                    nodealert=0
                    else:
                        nodealert=0
                else:
                    if abs(xyvel[-1])>Tvela1:
                        if abs(xytilt[-1]-xytilt[0])<=Ttilt:
                            nodealert=0
                        else:
                            if abs(xyvel[-1])<=Tvela2:
                                if abs(xzvel[-1])>op_axis_k*abs(xyvel[-1]):
                                    nodealert=1
                                else:
                                    nodealert=0
                            else:
                                if abs(xzvel[-1])>op_axis_k*abs(xyvel[-1]):
                                    nodealert=2
                                else:
                                    nodealert=0
                    else:
                        nodealert=0
                                
                out=[cur_node+1,nodealert,round(xztilt[-1]-xztilt[0],2),round(xzvel[-1],3),round(xytilt[-1]-xytilt[0],2),round(xyvel[-1],3)]
                print out
                csvout.append([now_time.strftime("%Y-%m-%d %H:%M"),loc_col_name[INPUT_which_sensor],
                                str(cur_node+1),
                                str(nodealert),
                                str(round(xztilt[-1]-xztilt[0],2)),
                                str(round(xzvel[-1],3)),
                                str(round(xytilt[-1]-xytilt[0],2)),
                                str(round(xyvel[-1],3))])
                if nodealert>=0:printfigures=printfigures+1
                       
            ##plots column position##
            ac_X,ac_XZ, ac_XY=accumulate_translate(allnodes_colpos_splinefit_X,allnodes_colpos_splinefit_XZ,
                                                    allnodes_colpos_splinefit_XY, num_nodes, INPUT_number_colpos,dates_to_plot,
                                                    loc_col_name[INPUT_which_sensor])
                            
            ##plots spline-fitted time series (tilt, velocity) within date range##
            for INPUT_which_axis in [0,1]:
                tiltvelfig=plt.figure(10+INPUT_which_axis)
                plt.clf()
                axtilt=tiltvelfig.add_subplot(121)
                axvel=tiltvelfig.add_subplot(122, sharex=axtilt)
                        
                if INPUT_which_axis==0:
                    tiltvelfig.suptitle(loc_col+" XZ as of "+str(now_time.strftime("%Y-%m-%d %H:%M")))

                else:
                    tiltvelfig.suptitle(loc_col+" XY as of "+str(now_time.strftime("%Y-%m-%d %H:%M")))

                for INPUT_which_node in range(num_nodes):
                    ##extracts data from array##
                    curnode_splinefit=allnodes_splinefit[INPUT_which_node]
                            
                    curaxsplinefit=curnode_splinefit[INPUT_which_axis]
                    subtilt=curaxsplinefit[0]
                    subdays=curaxsplinefit[1]
                    subtilt_fine=curaxsplinefit[2]
                    subdtilt_fine=curaxsplinefit[3]
                    subdays_fine=curaxsplinefit[4]
                    spline =curaxsplinefit[5]

                    subdates=day_to_date(subdays, now_time)
                    subdates_fine=day_to_date(subdays_fine, now_time)
                                            
                    axtilt.axhspan(offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))-thresholdtilt,offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))+thresholdtilt,color='0.9')
                    axtilt.axhline(y=(offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))),color='0.6')
                    axtilt.plot(subdates, [offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))+ (q-subtilt_fine[0]) for q in subtilt], '+-', color='0.4')
                    axtilt.plot(subdates_fine, [offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))+ (q-subtilt_fine[0]) for q in subtilt_fine], '-',linewidth=1,label="n"+str(INPUT_which_node+1))
                            

                    axvel=tiltvelfig.add_subplot(122, sharex=axtilt)
                            
                    offsetvel=0
                    axvel.axhspan(offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))-thresholdvel,offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))+thresholdvel,color='0.9')
                    axvel.axhline(y=(offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))),color='0.6')
                    axvel.plot(subdates_fine, [offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))+ q for q in subdtilt_fine], '-',linewidth=1,label="n"+str(INPUT_which_node+1))
                            

                    
                days_vlines=[now_time+timedelta(days=-q) for q in range(0,int(INPUT_fit_interval)+1)]
                for dvl in range(len(days_vlines)):
                    if dvl!=0:lw=(0.2)

                    else:lw=(1)
                    plt.sca(axtilt)
                    axtilt.axvline(x=days_vlines[dvl], color='r',lw=lw)
                    plt.sca(axvel)
                    axvel.axvline(x=days_vlines[dvl], color='r',lw=lw)

                for cpvl in range(len(dates_to_plot)):
                    plt.sca(axtilt)
                    axtilt.axvline(x=dates_to_plot[cpvl], color='b',lw=0.5)
                            
                plt.sca(axtilt)
                cax=plt.gca()
                cax.yaxis.set_major_locator(MaxNLocator(4))
                cax.yaxis.set_minor_locator(AutoMinorLocator(4))
                cax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d %H:%M')
                plt.xlim(now_time+timedelta(days=-INPUT_fit_interval),now_time+timedelta(days=0))    
                plt.ylabel("displacement (m)")
                plt.xlabel("date,time")
                        
                plt.sca(axvel)
                cax=plt.gca()
                axvel.axhline(y=0.005,color='y', ls=':',lw=3,label="A1: Slow")
                axvel.axhline(y=.50,color='r', ls=':',lw=3,label="A2: Mod")
                axvel.axhline(y=-0.005,color='y', ls=':',lw=3,)
                axvel.axhline(y=-.50,color='r', ls=':',lw=3,)
                        
                cax.yaxis.set_major_locator(MaxNLocator(4))
                cax.yaxis.set_minor_locator(AutoMinorLocator(4))
                cax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d %H:%M')
                plt.yscale('symlog', linthreshy=.01,linscaley=1)
                plt.ylim(-1,1)
                plt.xlabel("date,time")
                plt.ylabel("velocity (m/day)")
                plt.legend()
                cax.legend(ncol=1,loc="upper left", bbox_to_anchor=(1,1),prop=legend_font_props)

                tiltvelfig.autofmt_xdate()

                if INPUT_which_axis==0:
                    fig_name=OutputFigurePath+loc_col_name[INPUT_which_sensor]+"_xz.png"
                else:
                    fig_name=OutputFigurePath+loc_col_name[INPUT_which_sensor]+"_xy.png"

                #plt.savefig(fig_name, dpi=100, facecolor='w', edgecolor='w',orientation='landscape')
                    

            if which_sensor!=-1:        
                plt.show()
            else:continue
            
            csvout=np.asarray(csvout)
            with open(CSVOutputFile, "wb") as f:
                writer = csv.writer(f)
                writer.writerows(csvout)
                print "\nAlert file written."
                check_another_sensor=int(raw_input("Choose another sensor? (1) Yes     (2) No : "))

######################################################################
##                             MAIN                                 ##
######################################################################

##gets configuration from file##
cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')

InputFilePath = cfg.get('I/O','InputFilePath')
OutputFilePath = cfg.get('I/O','OutputFilePath')
OutputFigurePath = cfg.get('I/O','OutputFigurePath')
PrintFigures = cfg.getboolean('I/O','PrintFigures')
CSVOutputFile = cfg.get('I/O','CSVOutputFilePath') + cfg.get('I/O','CSVOutputFile')

GeneratePlots()







 
