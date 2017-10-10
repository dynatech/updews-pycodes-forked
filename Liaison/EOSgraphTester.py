import webbrowser

site = raw_input('site (Ex: agb,msl) : ')
column = raw_input('column (Ex: agbsb,ltetb) : ')
tdata = raw_input('from (Ex: 2017-08-15 19:30:00): ')
fdata = raw_input('to (Ex: 2017-08-18 19:30:00): ')
host ="swatqa"

webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/120/rain/%s/%s/%s'%(host,site,fdata,tdata), new = 2)
webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/120/subsurface/%s/%s/%s'%(host,column,'n',tdata), new = 2)
webbrowser.get('chrome %s').open('http://%s/data_analysis/Eos_onModal/120/surficial/%s/%s/%s'%(host,site,fdata,tdata), new = 2)