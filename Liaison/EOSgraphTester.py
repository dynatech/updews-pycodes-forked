import webbrowser
import time
site = raw_input('site (Ex: agb,msl) : ')
column_unfiltered = raw_input('column (Ex: agbsb,ltetb) : ')
column = column_unfiltered.split(" ")
tdata = raw_input('from (Ex: 2017-08-15 19:30:00): ')
fdata = raw_input('to (Ex: 2017-08-18 19:30:00): ')
host ="swatqa"


webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/120/rain/%s/%s/%s'%(host,site,fdata,tdata), new = 2)
for data in column:
    webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/120/subsurface/%s/%s/%s'%(host,data,'n',tdata), new = 2)

webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/120/surficial/%s/%s/%s'%(host,site,fdata,tdata), new = 2)

time.sleep(25)
webbrowser.get('chrome %s').open('http://swatqa/data_analysis/Eos_onModal/120/pdf/%s/%s'%(site,column_unfiltered), new = 2)