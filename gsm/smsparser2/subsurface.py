import sys,re
import pandas as pd
import numpy as np
import dynadb.db as dynadb
from datetime import datetime as dt
import smsclass


cols = ['site','timestamp','id', 'msgid', 'mval1', 'mval2']

prevdatetime = ['0','0','0']
backupGID=['0','0','0']
tempbuff =['0','0','0']
temprawlist=[]
buff=[]
SOMS=[]

def v1(sms):
    """
       - Process the sms message that fits for subsurface version 1 data.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **Dataframe**  - The Parse data of the message and into Dataframe structure.

    """    
    data = sms.msg
    data = data.replace("DUE","")
    data = data.replace(",","*")
    data = data.replace("/","")
    line = data[:-2]

   
    tsm_name = line[0:4]
    print 'SITE: ' + tsm_name
    ##msgdata = line[5:len(line)-11] #data is 6th char, last 10 char are date
    msgdata = (line.split('*'))[1]
    print 'raw data: ' + msgdata
    #getting date and time
    #msgdatetime = line[-10:]
    try:
        timestamp = (line.split('*'))[2][:10]
        print 'date & time: ' + timestamp
    except:
        print '>> Date and time defaults to SMS not sensor data'
        timestamp = sms.ts

    # col_list = cfg.get("Misc","AdjustColumnTimeOf").split(',')
    if tsm_name == 'PUGB':
        timestamp = sms.ts
        print "date & time adjusted " + timestamp
    else:
        timestamp = dt.strptime(timestamp,
            '%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
        print 'date & time no change'
        
    dlen = len(msgdata) #checks if data length is divisible by 15
    #print 'data length: %d' %dlen
    nodenum = dlen/15
    #print 'number of nodes: %d' %nodenum
    if dlen == 0:
        print 'Error: There is NO data!'
        return 
    elif((dlen % 15) == 0):
        #print 'Data has correct length!'
        valid = dlen
    else:
        print 'Warning: Excess data will be ignored!'
        valid = nodenum*15
        
    outl_tilt = []
    outl_soms = []
    try:    
        i = 0
        while i < valid:
            #NODE ID
            #parsed msg.data - NODE ID:
            node_id = int('0x' + msgdata[i:i+2],16)
            i=i+2
            
            #X VALUE
            #parsed msg.data - TEMPX VALUE:
            tempx = int('0x' + msgdata[i:i+3],16)
            i=i+3
            
            #Y VALUE
            #parsed msg.data - TEMPY VALUE:
            tempy = int('0x' + msgdata[i:i+3],16)
            i=i+3
            
            #Z VALUE
            #parsed msg.data - ZVALUE:
            tempz = int('0x' + msgdata[i:i+3],16)
            i=i+3
            
            #M VALUE
            #parsed msg.data - TEMPF VALUE:
            tempf = int('0x' + msgdata[i:i+4],16)
            i=i+4
            
            valueX = tempx
            if valueX > 1024:
                valueX = tempx - 4096

            valueY = tempy
            if valueY > 1024:
                valueY = tempy - 4096

            valueZ = tempz
            if valueZ > 1024:
                valueZ = tempz - 4096

            valueF = tempf #is this the M VALUE?


            tsm_name=tsm_name.lower()
            line_tilt = {"ts":timestamp,"node_id": node_id,"xval":valueX,"yval":valueY,"zval":valueZ}
            line_soms = {"ts":timestamp,"node_id": node_id,"mval1":valueF}
            outl_tilt.append(line_tilt)
            outl_soms.append(line_soms)
            
        df_tilt = smsclass.DataTable('tilt_'+tsm_name,pd.DataFrame(outl_tilt).set_index(['ts']))
        df_soms = smsclass.DataTable('soms_'+tsm_name,pd.DataFrame(outl_soms).set_index(['ts']))
        data = [df_tilt,df_soms]
        return data
      
    except KeyError:
        print '\n>>Error: Error in Data format'
        return 
    except KeyboardInterrupt:
        print '\n>>Error: Unknown'
        raise KeyboardInterrupt
        return
    except ValueError:
        print '\n>>Error: Unknown'
        return

def twos_comp(hexstr):
    """
       - Process the convertion of x, y and z data for subsurface version 2 data.
      
      :param hexstr: String dat of x, y or z .
      :type str: str
      :returns: **num - sub or num**  - Converted value.

    """
    num = int(hexstr[2:4]+hexstr[0:2],16)
    if len(hexstr) == 4:
        sub = 65536
    else:
        sub = 4096
    if num > 2048:  
        return num - sub
    else:
        return num

def v2(sms):
    """
       - Process the sms message that fits for subsurface version 2 data.
      
      :param sms: list data info of sms message .
      :type sms: list
      :returns: **Dataframe**  - The Parse data of the message and into Dataframe structure.

    """
    msg = sms.msg
    
    if len(msg.split(",")) == 3:
        print ">> Editing old data format"
        datafield = msg.split(",")[1]
        dtype = datafield[2:4].upper()
        if dtype == "20" or dtype == "0B":
            dtypestr = "x"
        elif dtype == "21" or dtype == "0C":
            dtypestr = "y"
        elif dtype == "6F" or dtype == "15":
            dtypestr = "b"
        elif dtype == "70" or dtype == "1A":
            dtypestr = "c"
        else:
            raise ValueError(">> Data type" + dtype + "not recognized")
            
        
        i = msg.find(",")
        msg = msg[:i] + "*" + dtypestr + "*" + msg[i+1:]
        msg = msg.replace(",","*").replace("/","")
        
    outl = []
    msgsplit = msg.split('*')
    tsm_name = msgsplit[0] # column id

    if len(msgsplit) != 4:
        print 'wrong data format'
        # print msg
        return

    if len(tsm_name) != 5:
        print 'wrong master name'
        return

    print msg

    dtype = msgsplit[1].upper()
   
    datastr = msgsplit[2]
    
    if len(datastr) == 136:
        datastr = datastr[0:72] + datastr[73:]
    
    ts = msgsplit[3].strip()
  
    if datastr == '':
        datastr = '000000000000000'
        print ">> Error: No parsed data in sms"
        return
   
    if len(ts) < 10:
       print '>> Error in time value format: '
       return
    
    ts_patterns = ['%y%m%d%H%M%S', '%Y-%m-%d %H:%M:%S']
    timestamp = ''
    ts = re.sub("[^0-9]","",ts)
    for pattern in ts_patterns:
        try:
            timestamp = dt.strptime(ts,pattern).strftime('%Y-%m-%d %H:%M:00')
            break
        except ValueError:
            print "Error: wrong timestamp format", ts, "for pattern", pattern
 
    if timestamp == '':
        raise ValueError(">> Error: Unrecognized timestamp pattern " + ts)

    # update_sim_num_table(tsm_name,sender,timestamp[:8])

 # PARTITION the message into n characters
    if dtype == 'Y' or dtype == 'X':
       n = 15
       # PARTITION the message into n characters
       sd = [datastr[i:i+n] for i in range(0,len(datastr),n)]
    elif dtype == 'B':
        # do parsing for datatype 'B' (SOMS RAW)
        outl = soms_parser(msg,1,10,0)    
        name_df = 'soms_'+tsm_name.lower()   
        # for piece in outl:
        #     print piece
    elif dtype == 'C':
        # do parsing for datatype 'C' (SOMS CALIB/NORMALIZED)
        outl = soms_parser(msg,2,7,0)
        name_df = 'soms_'+tsm_name.lower()  
        # for piece in outl:
        #     print piece
    else:
        raise IndexError("Undefined data format " + dtype )
    
    # do parsing for datatype 'X' or 'Y' (accel data)
    if dtype.upper() == 'X' or dtype.upper() =='Y':
        outl = []
        name_df = 'tilt_'+tsm_name.lower()
        for piece in sd:
            try:
                    # print piece
                ID = int(piece[0:2],16)
                msgID = int(piece[2:4],16)
                xd = twos_comp(piece[4:7])
                yd = twos_comp(piece[7:10])
                zd = twos_comp(piece[10:13])
                bd = (int(piece[13:15],16)+200)/100.0
                # line = [timestamp, ID, msgID, xd, yd, zd, bd]
                line = {"ts":timestamp, "node_id":ID, "type_num":msgID,
                "xval":xd, "yval":yd, "zval":zd, "batt":bd}
                outl.append(line)
            except ValueError:
                print ">> Value Error detected.", piece,
                print "Piece of data to be ignored"
                return
    
    df = pd.DataFrame(outl)
    df = df.set_index(['ts'])
    data = smsclass.DataTable(name_df,df)
    return  data
       
def log_errors(errortype, line, dt):
    error=""
    writefolder=''
    
    x = {
        0: 'wrong identifier', 1: 'wrong node division',
        2: '2nd text', 3: 'unidentified error', 4: 'no datetime',
        10: 'random character'
    }
    
    error = x[errortype] + '>' + str(dt)+ '>'+ line + '\n'
    #print(error)
    text_file= open(writefolder+'SOMS MSG ERRORS.txt','a')
    text_file.write(error)
    text_file.close()

def soms_parser(msgline,mode,div,err):
    """
       - Process the sms message that fits for soms data of version 2 and 3.
      
      :param msgline: Sms line of message for soms .
      :param mode: Mode of the data of soms.
      :param div: Soms division of data .
      :param err: Error line in the message.
      :type msgline: str
      :type mode: str
      :type div: str
      :type err: str
      :returns: **Dataframe**  - The Parse data of the message and into Dataframe structure.

    """
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
        log_errors(4,msgline,dt)
        return rawlist   
   
   #if msgdata is broken (without nodeid at start)   
    try:
        firsttwo = int('0x'+data[:2],base=0)
    except:
        firsttwo = data[:2]
        log_errors(10,msgline,dt) 
        
    if firsttwo in nodecommands:        # kapag msgid yung first 2 chars ng msgline
        log_errors(2,msgline,dt)
            
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
            log_errors(10, msgline, dt)
            continue
        try:    
            CMD = int('0x'+data[2+div*i:4+div*i],base=0)
        except:
            log_errors(10, msgline, dt)
            continue
        
        if CMD in nodecommands:
            if div==6:
                rawdata1 = np.NaN
            else:
                try:    
                    rawdata1= int('0x'+ data[6+div*i:7+div*i] 
                        + data[4+div*i:6+div*i], base=0)
                except:
                    log_errors(10,msgline,dt)
                    rawdata1=np.nan
        else:
            #print "WRONG DATAMSG:" + msgline +'/n err: '+ str(err)
            if mode == 1: 
                if err == 0: # err0: 'b' gives calib data
                    if CMD in [112,113,26]:
                        log_errors(0, msgline, dt)
                        return soms_parser(msgline,2,7,1)
                    else:
                        log_errors(1,msgline,dt)
                        return soms_parser(msgline,1,12,2)   #if CMD cannot be distinguished try 12 chars
                elif err == 1:
                    log_errors(1,msgline,dt)
                    return soms_parser(msgline,1,12,2)   # err: if data has 2 extra zeros
                elif err == 2:
                    log_errors(2,msgline,dt)
                    return rawlist
                else:
                    log_errors(3, msgline, dt)
                    return rawlist

            if mode == 2:
                if err == 0: #if c gives raw data
                    if CMD in [110, 111, 21]:
                        log_errors(0,msgline,dt)
                        return soms_parser(msgline,1,10,1) #if c gives raw data
                    else:
                        log_errors(1,msgline,dt)
                        #print "div=6!"
                        return soms_parser(msgline,2,6,2)    #wrong node division
                elif err == 1:
                    log_errors(1,msgline,dt)
                    return soms_parser(msgline,2,6,2)    #if CMD cannot be distinguished
                elif err == 2:
                    log_errors(2,msgline,dt)
                    return rawlist
                else:
                    log_errors(3,msgline,dt)
                    return rawlist
            if mode == 3:
                return rawlist

                
        if div == 10 or div == 12 or div == 15:           #if raw data
            try:
                rawdata2= int('0x' + data[9 + div*i:10 + div*i]
                    + data[7+ div*i:9 + div*i], base =0)
            except:
                log_errors(10, msgline, dt)
                rawdata2 = np.nan

        # rawlist.append([site, str(dt), GID, CMD, rawdata1, rawdata2])
        rawlist.append({"ts":str(dt), "node_id":GID, "type_num":CMD, "mval1":rawdata1, "mval2":rawdata2})
  
    if len(data)%div!=0:

        prevdatetime[a]=r[3][:10]
        backupGID[a]=data[maxnode*div:2+div*maxnode]
        if len(tempbuff[a])>1:
            temprawlist = rawlist
            buff = soms_parser(tempbuff[a],1,10,0)
            #print temprawlist+buff
            return temprawlist+buff
            

    return rawlist
