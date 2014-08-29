import pandas as pd
import ConfigParser
from timelyTrigger import *

cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')

columnproperties_file = cfg.get('I/O','ColumnProperties')
columnproperties_path = cfg.get('I/O','ColumnPropertiesPath')
purged_path = cfg.get('I/O','InputFilePath')
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring')
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')

def main():
    #2. getting all column properties
    sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

    dfw_all = pd.DataFrame()
    alertSms = ""
    for s in range(len(sensors)):
        #if s!=7: continue
        #3. getting current column properties
        colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]

        fname = '%s%s\%s alert.csv' % (proc_monitoring_path,colname,colname)
        df = pd.read_csv(fname,names = ['ts','id','unk','xzd','xyd','da','minv','maxv','va','na','ana'], parse_dates=[0],index_col=0)
        df2 = df[['id','na','ana']]
        alertdf = df2[((df2['na']=='a1') | (df2['na']=='a2'))& (df2.index==df2.index[-1])]
        
        if len(alertdf) > 0:
            alertSms = alertSms + colname + "\n"
            for line in alertdf.to_string().split('\n')[2:]:
                alertSms = alertSms + line + "\n"
                
        
        dfw = df2[((df2['na']!='a0') | (df2['ana']!='a0')) & (df2.index==df2.index[-1])]
        dfw = pd.concat([pd.Series(colname,index=dfw.index,name="site"),dfw],axis=1)
        if (len(dfw_all) == 0):
            dfw_all = dfw
        else:
            dfw_all = dfw_all.append(dfw)
        

    print alertSms
    
    f = open(proc_monitoring_path+'alerts\\alertSMS.txt', 'w')
    f.write(alertSms)
    f.close()
    
    print dfw_all
    dfw_all.to_csv("D:\\dewslandslide\\alerts\\alertStatus.csv")

if __name__ == '__main__':
    main()
        
