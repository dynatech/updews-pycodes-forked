from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np

def get_rt_window(rt_window_length,roll_window_size):
    
    ##DESCRIPTION:
    ##returns the time interval for real-time monitoring

    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    ##roll_window_size; integer; number of data points to cover in moving window operations
    
    ##OUTPUT: 
    ##end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

    ##set current time as endpoint of the interval
    end=datetime.now()

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
    offsetstart=end-timedelta(days=rt_window_length+((roll_window_size+1)/48.))
    
    return end, start, offsetstart


def nodes_to_columns(df, num_nodes):
    #creating series list
    xzlist=[]
    xylist=[]
    #xlist=[]
    for curnodeID in range(num_nodes):
        #extracting data from current node, with good tilt filter
        df_curnode=df[(df.Node_ID==curnodeID+1)]
        dates=df_curnode.index

        #handling "no data"
        if len(dates)<1:
            print curnodeID 
            xzlist.append(pd.Series(data=[0],index=[df.index[0]]))
            xylist.append(pd.Series(data=[0],index=[df.index[0]]))
            #xlist.append(pd.Series(data=[seg_len],index=[df.index[0]]))
            continue

        #extracting component displacements as series
        xz=pd.Series(data=df_curnode['xzlin'], index=df_curnode.index, name=curnodeID+1)
        xy=pd.Series(data=df_curnode['xylin'], index=df_curnode.index, name=curnodeID+1)
        x=pd.Series(data=df_curnode['xlin'], index=df_curnode.index, name=curnodeID+1)
        
        #resampling series to 30-minute intervals
        xz=xz.resample('30Min',how='mean',base=0)
        xy=xy.resample('30Min',how='mean',base=0)
        x=x.resample('30Min',how='mean',base=0)
        
        #appending resampled series to list
        xzlist.append(xz)
        xylist.append(xy)
        xlist.append(x)

    #creating unfilled XZ, XY and X dataframes
    xzdf=pd.concat([xzlist[num_nodes-a-1] for a in range(num_nodes)] ,axis=1,join='outer', names=[num_nodes-b for b in range(num_nodes)])
    xydf=pd.concat([xylist[num_nodes-a-1] for a in range(num_nodes)] ,axis=1,join='outer', names=[num_nodes-b for b in range(num_nodes)])
    xdf=pd.concat([xlist[num_nodes-a-1] for a in range(num_nodes)] ,axis=1,join='outer', names=[num_nodes-b for b in range(num_nodes)])  
    return xzdf, xydf, xdf







    

##set/get values from config file
rt_window_length=3.
roll_window_size=7
columnproperties_path='/home/jun/SVN/updews-pycodes/Stable Versions/'
columnproperties_file='column_properties.csv'
columnproperties_headers=['colname','num_nodes','seg_len']
purged_path='/home/jun/Dropbox/Senslope Data/Purged/New/'
purged_file='_.csv'










#MAIN
end, start, offsetstart=get_rt_window(rt_window_length,roll_window_size)

sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

for s in range(len(sensors)):
    #getting current column properties
    colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]
    
    print "\nDATA for ",colname," as of ", end.strftime("%Y-%m-%d %H:%M")

    #importing purged csv file of current column to dataframe
    purged=pd.read_csv(purged_path+colname+purged_file,header=0,parse_dates=[1],index_col=[2])
    #removing unnecessary column
    purged=purged.ix[:, 1:]
    #print purged
    

    #extracting last data per node
    last_data=pd.DataFrame(data=None)
    for n in range(1,1+num_nodes):
        last_data=last_data.append(purged[(purged.index.values==n)].tail(1), ignore_index=False)
    print last_data

    
    #getting dataframe rows within real-time window
    purged=purged[(purged['ts']>=offsetstart)]

    
    print purged

    break
    
    


    
