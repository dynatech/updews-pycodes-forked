from datetime import datetime, date, time, timedelta

def get_rt_window(rt_window_length,roll_window_size):
    ##DESCRIPTION:
    ##returns the time interval for real-time monitoring

    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    ##roll_window_size; integer; number of data points to cover in moving window operations
    
    ##OUTPUT: 
    #end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

    ##setting current time as endpoint of the interval
    end=datetime.now()

    ##round down current time to the nearest HH:00 or HH:30 time value
    end=time_exact_interval(end)
    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    #actual starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+((roll_window_size+1)/48.)
    
    return end, start, offsetstart
    

##get values from config file
rt_window_length=3.
roll_window_size=7

end, start, offsetstart=get_rt_window(rt_window_length,roll_window_size)
print end, start, offsetstart 
    