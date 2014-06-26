from datetime import datetime, date, time, timedelta
import pandas as pd

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


def df_from_csv(purgedfilepath,colname,col,usecol):
    print csvfilepath+colname+"_proc.csv"
    df=pd.read_csv(csvfilepath+colname+"_proc.csv",names=col,usecols=usecol,parse_dates=[col[0]],index_col=col[0])

    xz,xy = df['xz'].values,df['xy'].values
    x = x_from_xzxy(seg_len, xz, xy)
        
    #appending linear displacements series to data frame
    df['xlin']=pd.Series(data=x,index=df.index)
    df['xzlin']=pd.Series(data=xz,index=df.index)
    df['xylin']=pd.Series(data=xy,index=df.index)
    df.drop(['xz','xy'],inplace=True,axis=1)
    gc.collect()
    
    #creating dataframes
    xzdf, xydf, xdf = create_dataframes(df, num_nodes, seg_len)

    return xzdf, xydf, xdf






    

##set/get values from config file
rt_window_length=3.
roll_window_size=7
columnproperties_path='/home/dynaslope-l5a/SVN/Dynaslope/updews-pycodes/Stable Versions/'
columnproperties_file='column_properties.csv'
columnproperties_headers=['colname','num_nodes','seg_len']
purged_path='/home/dynaslope-l5a/Dropbox/Senslope Data/Purged/New/'
purged_file='_.csv'










#MAIN
end, start, offsetstart=get_rt_window(rt_window_length,roll_window_size)

sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

for s in range(len(sensors)):
    colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]
    #print colname, num_nodes, seg_len

    print "\nDATA for ",colname," as of ", end.strftime("%Y-%m-%d %H:%M")

    purged=pd.read_csv(purged_path+colname+purged_file,header=0,parse_dates=[1],index_col=0)
    print purged[(purged['ts']>=start)]

    break
    
    


    
