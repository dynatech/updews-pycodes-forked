# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 10:07:29 2016

@author: SENSLOPEY
"""

import pandas as pd
import numpy as np

cols = ['site','timestamp','id', 'msgid', 'mval1', 'mval2']

prevdatetime = ['0','0','0']
backupGID=['0','0','0']
tempbuff =['0','0','0']
temprawlist=[]
buff=[]
SOMS=[]

def errorlogs(errortype, line, dt):
    error=""
    writefolder=''
    
    x = {
        0: 'wrong identifier',1: 'wrong node division',
        2: '2nd text',3: 'unidentified error',4: 'no datetime',10: 'random character'
    }
    
    error = x[errortype] + '>' + str(dt)+ '>'+ line + '\n'
    #print(error)
    text_file= open(writefolder+'SOMS MSG ERRORS.txt','a')
    text_file.write(error)
    text_file.close()

def somsparser(msgline,mode,div,err):
#    global prevdatetime
    global backupGID
    global tempbuff
    global temprawlist
    siteptr={'NAGSAM':1, 'BAYSBM':0}
    rawlist=[]
    rawdata1=0
    rawdata2=0
    if mode == 1: #if raw
        '''use following'''
        nodecommands = [110, 111, 21]
        maxnode= 13
    if mode == 2: #if calib
        '''use following'''
        nodecommands = [112, 113, 26]
        maxnode = 19
    if mode == 3:
        nodecommands = [110, 111, 21, 112, 113, 26 ]
        maxnode = 9
        
    r = msgline.split('*')
    site = r[0]
    data = r[2]    
    if site in ['NAGSAM', 'BAYSBM']:
        a = siteptr[site]
    else:
        a = 2
    try:      
        dt=pd.to_datetime(r[3][:12],format='%y%m%d%H%M%S') #uses datetime from end of msg 
    except:
        dt='0000-00-00 00:00:00'
        errorlogs(4,msgline,dt)
        return rawlist   
   
   #if msgdata is broken (without nodeid at start)   
    try:
        firsttwo = int('0x'+data[:2],base=0)
    except:
        firsttwo = data[:2]
        errorlogs(10,msgline,dt) 
        
    if firsttwo in nodecommands:        # kapag msgid yung first 2 chars ng msgline
        errorlogs(2,msgline,dt)
            
        if long(r[3][:10])-long(prevdatetime[a])<=10:
            data=backupGID[a]+r[2]
            #print 'data: ' + data
        else: #hanap next line na pareho
            tempbuff[a] = msgline
            return []

    #parsing msgdata
    for i in range (0, int(len(data)/div)):
        try:
            GID=int("0x"+data[i*div:2+div*i],base=0)
        except: #kapag hindi kaya maging int ng gid
            errorlogs(10, msgline, dt)
            continue
        try:    
            CMD = int('0x'+data[2+div*i:4+div*i],base=0)
        except:
            errorlogs(10, msgline, dt)
            continue
        
        if CMD in nodecommands:
            if div==6:
                rawdata1 = np.NaN
            else:
                try:    
                    rawdata1= int('0x'+ data[6+div*i:7+div*i]+data[4+div*i:6+div*i], base=0)
                except:
                    errorlogs(10,msgline,dt)
                    rawdata1=np.nan
        else:
            #print "WRONG DATAMSG:" + msgline +'/n err: '+ str(err)
            if mode == 1: 
                if err == 0: # err0: 'b' gives calib data
                    if CMD in [112,113,26]:
                        errorlogs(0, msgline, dt)
                        return somsparser(msgline,2,7,1)
                    else:
                        errorlogs(1,msgline,dt)
                        return somsparser(msgline,1,12,2)   #if CMD cannot be distinguished try 12 chars
                elif err == 1:
                    errorlogs(1,msgline,dt)
                    return somsparser(msgline,1,12,2)   # err: if data has 2 extra zeros
                elif err == 2:
                    errorlogs(2,msgline,dt)
                    return rawlist
                else:
                    errorlogs(3, msgline, dt)
                    return rawlist

            if mode == 2:
                if err == 0: #if c gives raw data
                    if CMD in [110, 111, 21]:
                        errorlogs(0,msgline,dt)
                        return somsparser(msgline,1,10,1) #if c gives raw data
                    else:
                        errorlogs(1,msgline,dt)
                        #print "div=6!"
                        return somsparser(msgline,2,6,2)    #wrong node division
                elif err == 1:
                    errorlogs(1,msgline,dt)
                    return somsparser(msgline,2,6,2)    #if CMD cannot be distinguished
                elif err == 2:
                    errorlogs(2,msgline,dt)
                    return rawlist
                else:
                    errorlogs(3,msgline,dt)
                    return rawlist
            if mode == 3:
                return rawlist

                
        if div == 10 or div == 12 or div == 15:           #if raw data
            try:
                rawdata2= int('0x' + data[9+div*i:10+div*i]+data[7+div*i:9+div*i], base =0)
            except:
                errorlogs(10,msgline,dt)
                rawdata2=np.nan

        rawlist.append([site, str(dt),GID,CMD,rawdata1,rawdata2])

  
    if len(data)%div!=0:

        prevdatetime[a]=r[3][:10]
        backupGID[a]=data[maxnode*div:2+div*maxnode]
        if len(tempbuff[a])>1:
            temprawlist = rawlist
            buff = somsparser(tempbuff[a],1,10,0)
            #print temprawlist+buff
            return temprawlist+buff
            

    return rawlist


#error=" "
#prevdatetime = ['0','0']
#backupGID=['0','0']
#tempbuff = ['0','0']
#
#temprawlist=[]
#buff=[]
#SOMS=[]
#line = ""    
#    
#while line != 'exit':
#    
#    line= raw_input('Enter line: ')  
#     
#    if '*b*' in line:
#        SOMS = somsparser(line,1,10,0)
#    if '*c*' in line:
#        SOMS = somsparser(line,2,7,0)
#
#    soms=pd.DataFrame.from_records(SOMS,columns=cols)
#    print(soms)

