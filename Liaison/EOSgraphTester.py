import webbrowser
import time
site = raw_input('site (Ex: par) : ')
column_unfiltered = raw_input('column (Ex: parta partb) : ')
column = column_unfiltered.split(" ")
fdata = raw_input('from (Ex: 2017-08-15 19:30:00): ')
tdata = raw_input('to (Ex: 2017-08-18 08:30:00): ')
host ="swatqa"
num="121"

webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/%s/rain/%s/%s/%s'%(host,num,site,fdata,tdata), new = 2)
for data in column:
    webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/%s/subsurface/%s/%s/%s'%(host,num,data,'n',tdata), new = 2)

webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/%s/surficial/%s/%s/%s'%(host,num,site,fdata,tdata), new = 2)

time.sleep(25)
webbrowser.get('chrome %s').open('http://swatqa/data_analysis/Eos_onModal/%s/pdf/%s'%(site,column_unfiltered), new = 2)