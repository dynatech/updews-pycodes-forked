import sys,re
import dynadb.db as dynadb
from datetime import datetime as dt

class sms:
    def __init__(self,num,sender,data,ts):
       self.num = num
       self.simnum = sender
       self.msg = data
       self.ts = dt

def v1_due(sms):
    data = sms.data
    data = data.replace("DUE","")
    data = data.replace(",","*")
    data = data.replace("/","")
    data = data[:-2]
    return data

def v1(sms):
    line = v1_due(sms)
    txtdatetime = sms.dt
    sender = sms.simnum

   
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
        timestamp = txtdatetime

    # col_list = cfg.get("Misc","AdjustColumnTimeOf").split(',')
    if tsm_name == 'PUGB':
        timestamp = txtdatetime
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
        
    # dynadb.update_sim_num_table(tsm_name,sender,timestamp[:10])
        
    query_tilt = ("INSERT IGNORE INTO tilt_%s (ts,node_id,xval,yval,zval) "
        "VALUES " % (str(tsm_name.lower()))
        )
    query_soms = ("INSERT IGNORE INTO soms_%s (ts,node_id,mval1) "
        "VALUES " % (str(tsm_name.lower()))
        )
    
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

            query_tilt += ("('%s',%d,%d,%d,%d)," % (str(timestamp), 
                node_id,valueX,valueY,valueZ)
                )
            query_soms += "('%s',%d,%d)," % (str(timestamp),node_id,valueF)

            print "%s\t%s\t%s\t%s\t%s" % (str(node_id), str(valueX),
                str(valueY), str(valueZ), str(valueF))
            
        query_tilt = query_tilt[:-1]
        query_soms = query_soms[:-1]

        # print query_tilt
        # print query_soms
        
        # print query

        if i!=0:
        #     # dbio.create_table(str(tsm_name), "sensor v1")
        #     dbio.commit_to_db(query_tilt, 'process_column_v1')
            dynadb.write(query_tilt, 'process_column_v1')
            dynadb.write(query_soms, 'process_column_v1')
        
        # spawn_alert_gen(tsm_name,timestamp)
                
    except KeyboardInterrupt:
        print '\n>>Error: Unknown'
        raise KeyboardInterrupt
        return
    except ValueError:
        print '\n>>Error: Unknown'
        return

def twos_comp(hexstr):
    # print hexstr
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
    msg = sms.data
    sender = sms.simnum
    txtdatetime = sms.dt
    
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

        # dynadb.update_sim_num_table(tsm_name,sender,timestamp[:8])

     # PARTITION the message into n characters
        if dtype == 'Y' or dtype == 'X':
           n = 15
           # PARTITION the message into n characters
           sd = [datastr[i:i+n] for i in range(0,len(datastr),n)]
        elif dtype == 'B':
            # do parsing for datatype 'B' (SOMS RAW)
            outl = ssp.soms_parser(msg,1,10,0)       
            for piece in outl:
                print piece
        elif dtype == 'C':
            # do parsing for datatype 'C' (SOMS CALIB/NORMALIZED)
            outl = ssp.soms_parser(msg,2,7,0)
            for piece in outl:
                print piece
        else:
            raise IndexError("Undefined data format " + dtype )
        
        # do parsing for datatype 'X' or 'Y' (accel data)
        if dtype.upper() == 'X' or dtype.upper() =='Y':
            outl = []
            for piece in sd:
                try:
                    # print piece
                    ID = int(piece[0:2],16)
                    msgID = int(piece[2:4],16)
                    xd = twos_comp(piece[4:7])
                    yd = twos_comp(piece[7:10])
                    zd = twos_comp(piece[10:13])
                    bd = (int(piece[13:15],16)+200)/100.0
                    line = [tsm_name,timestamp,ID,msgID,xd,yd,zd,bd]
                    print line
                    outl.append(line)
                except ValueError:
                    print ">> Value Error detected.", piece,
                    print "Piece of data to be ignored"
        
        # spawn_alert_gen(tsm_name,timestamp)

        return outl
