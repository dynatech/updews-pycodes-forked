from datetime import datetime, timedelta, date, time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import Tkinter as tk
import ttk

import querySenslopeDb as q
import RainfallAlert as lib

def get_end_ts():
    
    end = datetime.now()

    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    return end

def popupmsg(msg):
    popup = tk.Tk()
    popup.wm_title("Rainfall Notification")
    label = ttk.Label(popup, text=msg, font=("Verdana", 15))
    label.pack(side="top", fill="x", pady=10)
    ttk.Button(popup, text="Okay", command = popup.destroy).pack()
    popup.mainloop()

def to_write_notif(site, end):
    
    try:
        query = "SELECT * FROM rainfall_notif"
        query += " WHERE ts >= '%s' AND site = %s" %(end - timedelta(1), site)
        query += " ORDER BY ts DESC LIMIT 1"
        notif = q.GetDBDataFrame(query)
    except:
        notif = pd.DataFrame()
    
    if len(notif) == 0 or end.time() in [time(3,30), time(7,30), time(11,30),
                time(15,30), time(19,30), time(23,30)]:        
        write_notif = True    
    else:
        write_notif = False
    
    return write_notif
    
def notif_to_db(site, end):
    notif = pd.DataFrame({'site': [site], 'end': [end]})
    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
    notif.to_sql(name = 'rainfall_notif', con = engine, if_exists = 'append', schema = q.Namedb, index = False)

def cml_per_site(rain_gauge, end):
    gauge = rain_gauge['name'].values[0]
    dist = rain_gauge['dist'].values[0]
    data = lib.GetResampledData(gauge, end-timedelta(3), end-timedelta(3), end)
    try:
        one, three = lib.onethree_val_writer(data, end)
    except:
        one, three = 0, 0
    cml = pd.DataFrame({'rain_gauge': [gauge], 'dist': [dist], 'one': [one], 'three': [three]})
    return cml

def notif(rainprops, end):
    name = rainprops['name'].values[0]

    if to_write_notif(name, end):
        twoyrmax = rainprops['max_rain_2year'].values[0]
        halfmax=twoyrmax/2
    
        rain_gauges = rainprops[['RG1', 'RG2', 'RG3']].values[0]
        dist = rainprops[['d_RG1', 'd_RG2', 'd_RG3']].values[0]
        rain_gauges = pd.DataFrame({'name': rain_gauges, 'dist': dist})
        rain_gauges = rain_gauges.dropna()
        ind_rain_gauge = rain_gauges.groupby('name', as_index=False)
        cml = ind_rain_gauge.apply(cml_per_site, end=end)
            
        cml = cml[(cml.one >= halfmax) | (cml.three >= twoyrmax)].sort_values('dist')

        if len(cml) != 0:
            notif_to_db(name, end)
            popup_notif = pd.DataFrame({'site': name, 'rain_gauge': [cml['rain_gauge'].values[0]]})
            return popup_notif
            
    popup_notif = pd.DataFrame({'site': [name], 'rain_gauge': [np.nan]})
    return popup_notif
 
def main():
    end = get_end_ts()
    
    rainprops = q.GetRainProps('rain_props')  
    siterainprops = rainprops.groupby('name', as_index=False)
    
    popup_notif = siterainprops.apply(notif, end=end)
    popup_notif = popup_notif.dropna()
    
    if len(popup_notif) != 0:
        text = 'Check the following rainfall data:\n'
        text += popup_notif[['site', 'rain_gauge']].to_csv(index=False, header=False, sep=':')
        text = text.replace(':', ': ')
        popupmsg(text)
        
if __name__ == "__main__":
    main()
