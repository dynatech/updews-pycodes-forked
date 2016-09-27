import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator,MultipleLocator
import matplotlib.colors as colors
import matplotlib.cm as cmx
import ConfigParser
from scipy import stats
from datetime import datetime, date, time, timedelta
import sys
import os
from querySenslopeDb import *

def up_one(p):
    #INPUT: Path or directory
    #OUTPUT: Parent directory
    out = os.path.abspath(os.path.join(p, '..'))
    return out  

def GetGroundDF():
    try:

        query = 'SELECT timestamp, meas_type, site_id, crack_id, observer_name, meas, weather, reliability FROM gndmeas'
        
        df = GetDBDataFrame(query)
        return df
    except:
        raise ValueError('Could not get sensor list from database')

def replace_nin(x):
    if x == 'Messb':
        return 'mes'
    elif x == 'Nin':
        return 'mes'
    else:
        return x

def plot_cracks(which_site,zeroed=True):
    path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    out_path = up_one(path2)
    print_out_path = out_path + "/MonitoringOutput/GrndMeasPlots/"
    if not os.path.exists(print_out_path):
        os.makedirs(print_out_path)

    all_sites = []
    all_features = []
    all_num_data = []
    all_max_feature_name = []

    p_value = []
    
    df = GetGroundDF()

    print df
#    df=df[df['meas']!=np.nan]
    df = df[df['meas']<1000]
#    df=df[df['timestamp']!=' ']
    df['timestamp'] = [d.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(d) else '' for d in df['timestamp']]
    df=df[df['site_id']!=' ']
    df=df[df['crack_id']!=np.nan]
    df['timestamp']=pd.to_datetime(df['timestamp'])
    df=df.dropna(subset=['meas'])
    print np.unique(df['site_id'].values)

    df['site_id'] = map(lambda x: x.lower(),df['site_id'])
    df['crack_id'] = map(lambda x: x.title(),df['crack_id'])

    df = df.sort_values(['timestamp'])

    sitelist=np.unique(df['site_id'].values)    
    
    fig,ax=plt.subplots(nrows=len(which_site),ncols=1,sharex=True)
    fig.set_size_inches(15,8)
    ax_ind=0
    min_date=max(df['timestamp'].values)
    max_date=min(df['timestamp'].values)
    

    
    for s in range(len(sitelist)):
        if sitelist[s].lower() not in which_site:continue
        if len(which_site) != 1:
            curax=ax[ax_ind]
        else:
            curax = ax
        #getting current site and sorting according to date
        cursite=df[df['site_id']==sitelist[s]]
        cursite.sort(['timestamp','crack_id'],inplace=True)
        cursite.drop_duplicates(subset = ['timestamp','crack_id'],inplace=True)
        cursite[['meas']]=cursite[['meas']].astype(float)
        print max(cursite['timestamp'].values)
        print cursite.tail(20)

        if min(cursite['timestamp'].values)<min_date:min_date=min(cursite['timestamp'].values)
        if max(cursite['timestamp'].values)>max_date:max_date=max(cursite['timestamp'].values)

        print min_date, max_date

        
        all_sites.append(np.unique(cursite['site_id'].values)[0])
        
        features=np.unique(cursite['crack_id'].values)
        print cursite
        print features
        
        site_features = []
        site_p = []
        site_num_data = []
        site_max_feature_name = 0
        if len(features)>1:
            #generating non-repeating colors for features######## 
            jet = cm = plt.get_cmap('jet') 
            cNorm  = colors.Normalize(vmin=0, vmax=len(features)-1)
            scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=jet)
            colorVal=scalarMap.to_rgba(np.arange(len(features)))

        #generating series of markers
        marker=['x','d','+','s','*']
       
        for f in range(len(features)):
            #getting current feature
            curfeature=cursite[cursite['crack_id']==features[f]]
            if site_max_feature_name < len(features[f]):
                site_max_feature_name = len(features[f])
            #curfeature=cursite[cursite.loc[:,'crack_id']==features[f]]
         
            curfeature['timestamp']=pd.to_datetime(curfeature['timestamp'])
            
#            end_p = curfeature['timestamp'].iloc[-1]
#            start_p = end_p - timedelta(days=7)            
#            crack_data = curfeature[curfeature['timestamp']>=start_p]
            
            #p_value data computation
            crack_data = curfeature.tail(4)
            
            crack_data['delta'] = (crack_data['timestamp']-crack_data['timestamp'].values[0])
            crack_data['t'] = crack_data['delta'].apply(lambda x: x  / np.timedelta64(1,'D'))            
            
            
            data = crack_data['meas'].values
            time_data = crack_data['t'].values
            
            try:
                if abs(data[-2]-data[-1]) <= 1 or True:
                    m, b, r, p, std = stats.linregress(time_data,data)            
                    site_p.append(p)
                    site_num_data.append(len(data))
                    site_features.append(np.unique(curfeature['crack_id'].values)[0])
                    print crack_data
                    print m, "slope"
                    print p, "p value"
            except IndexError:
                print str(features[f]) + 'Index out of bounds'
            
#            for i in  np.arange(len(crack_data)):
            
            
            
            
            
            if zeroed:
                #getting zeroed displacement
                curfeature['meas_0']=curfeature['meas'].values-curfeature['meas'].values[0]
                #plotting zeroed displacement
                curax.plot(curfeature['timestamp'].values,curfeature['meas_0'],marker=marker[f%len(marker)],color=colorVal[f],label=features[f])
                curax.grid(True)
                print features[f]
            else:
                curax.plot(curfeature['timestamp'].values,curfeature['meas'],marker=marker[f%len(marker)],color=colorVal[f],label=features[f])
                curax.grid(True)
#                curfeature.plot('timestamp','Measurement',marker=marker[f%len(marker)],color=colorVal[f],label=features[f],ax=curax)
                print features[f]
        all_max_feature_name.append(site_max_feature_name)
        all_features.append(site_features)
        p_value.append(site_p)
        all_num_data.append(site_num_data)
        
        curax.set_xlabel('')
        curax.set_ylabel(sitelist[s].upper(),fontsize = 15)
        
        curax.legend(fontsize=10,loc='best',fancybox = True,framealpha = 0.5)
        ax_ind=ax_ind+1

        curax.set_xlim(min_date,max_date)
        plt.xticks(rotation = 45)
        plt.savefig(print_out_path+"/"+str(sitelist[s])+' Ground Data Plot.png', dpi=600, facecolor ='w', edgecolor = 'w', orientation = 'landscape',mode = 'w',pad_inches = 0.5,bbox_inches = 'tight')
        
            
    
    fig.tight_layout(pad = 2)
    for i in range(len(all_sites)):
        print "\nSite: {}".format(all_sites[i].title())
        for k in range(all_max_feature_name[i]+41):
            sys.stdout.write('#')
        print ""
        print "# {:<{}s} #   p value  #      Result     #".format('Feature',all_max_feature_name[i])
        for k in range(all_max_feature_name[i]+41):
            sys.stdout.write('-')
        print ""
        for j in range(len(p_value[i])):
#            print str(all_features[i][j]) + '\t' + str(p_value[i][j]) +'\t'+str(all_num_data[i][j])+'\n'
            if p_value[i][j] < 0.05:
                result = 'Significant'
            else:
                result = 'Not Significant'
            print "#   {:<5} #   ".format(str(all_features[i][j]),all_max_feature_name[i]) + "{:<6}   # ".format(str(format(round(p_value[i][j],4),'.4f'))) + "{:<15} # ".format(result)
        for k in range(all_max_feature_name[i]+41):
            sys.stdout.write('#')
        print ""

    plt.show()
    
cfg = ConfigParser.ConfigParser()
cfg.read('plotter-config.txt')  
#################################################################    

print "############################################################"
print "##               DEWS-L Ground Data Plotter               ##"
print "############################################################\n"

df = GetGroundDF()
        

df=df[df['site_id']!=' ']
df['site_id'] = map(lambda x: x.lower(),df['site_id'])
choices = list(np.unique(df['site_id'].values))
num_add = len(choices)%5
if num_add != 0:
    for i in range(5-num_add):
        choices.append(" ")
split = len(choices)/5
l1 = choices[0:split]
l2 = choices[split:split*2]
l3 = choices[split*2:split*3]
l4 = choices[split*3:split*4]
l5 = choices[split*4:split*5]


print "\nChoose among the following sites:"
print "########################################"
for f1, f2, f3, f4, f5 in zip(l1,l2,l3,l4,l5):
    print "## {0:<6s} {1:<6s} {2:<6s} {3:<6s} {4:<6s} ##".format(f1,f2,f3,f4,f5)
print "########################################"
print "Max of 4 sites separated by a comma. Ex. 'Nin, Bat'"

while True:
    sitelist = raw_input("Sites to be analyzed: ")
    sitelist = sitelist.replace(' ', '')
    sitelist = sitelist.lower()
    slist = sitelist.split(',')
    for i in slist:
        if not (i in choices):
            print "No site '{}' in the database.".format(i)    
            
    if len(slist) > 4:
        print "{} sites chosen. Please choose only a maximum of 4 sites.".format(len(slist))
        print "Check your input and try again."
        continue

    if not(i in choices):
        print "Please check you input and try again"
        continue
    
    else:
        break

while True:
    zeroed = raw_input("Do you want the initial measurements to be set at 0? (Y or N): ")
    zeroed = zeroed.title()
    zeroed = zeroed[0]
    if (zeroed != 'Y') and (zeroed != 'N'):
        print "Please answer 'Yes' or 'No'"
        continue
    else:
        if zeroed == 'Y':
            zeroed = True
        else:
            zeroed = False
        break



configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()

try:
    cfg.read(configFile)
    
    DBIOSect = "DB I/O"
    Hostdb = cfg.get(DBIOSect,'Hostdb')
    Userdb = cfg.get(DBIOSect,'Userdb')
    Passdb = cfg.get(DBIOSect,'Passdb')
    Namedb = cfg.get(DBIOSect,'Namedb')
    NamedbPurged = cfg.get(DBIOSect,'NamedbPurged')
    printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')
    
    valueSect = 'Value Limits'
    xlim = cfg.get(valueSect,'xlim')
    ylim = cfg.get(valueSect,'ylim')
    zlim = cfg.get(valueSect,'zlim')
    xmax = cfg.get(valueSect,'xmax')
    mlowlim = cfg.get(valueSect,'mlowlim')
    muplim = cfg.get(valueSect,'muplim')
    islimval = cfg.getboolean(valueSect,'LimitValues')
    
    PrintColProps = cfg.get('I/O', 'PrintColProps')
except:
    #default values are used for missing configuration files or for cases when
    #sensitive info like db access credentials must not be viewed using a browser
    #print "No file named: %s. Trying Default Configuration" % (configFile)
    Hostdb = "192.168.1.102"
    Userdb = "root"
    Passdb = "senslope"
    Namedb = "senslopedb"
    NamedbPurged = "senslopedb_purged"
    printtostdout = False
    
    xlim = 100
    ylim = 1126
    zlim = 1126
    xmax = 1200
    mlowlim = 2000
    muplim = 4000
    islimval = True   

plot_cracks(slist,zeroed)

