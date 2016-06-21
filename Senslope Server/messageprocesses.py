import os,time,serial,re,sys
import MySQLdb, subprocess
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio
import groundMeasurements as gndmeas
import SomsServerParser as SSP
import senslopeServer as server
import cfgfileio as cfg
import argparse
import queryserverinfo as qsi

def updateLastMsgReceivedTable(txtdatetime,name,sim_num,msg):
    query = """insert into senslopedb.last_msg_received
                (timestamp,name,sim_num,last_msg)
                values ('%s','%s','%s','%s')
                on DUPLICATE key update
                timestamp = '%s',
                sim_num = '%s',
                last_msg = '%s'""" %(txtdatetime,name,sim_num,msg,txtdatetime,sim_num,msg)
                
    dbio.commitToDb(query, 'updateLastMsgReceivedTable')
    
def updateSimNumTable(name,sim_num,date_activated):
    db, cur = dbio.SenslopeDBConnect('local')
    
    while True:
        try:
            query = """select sim_num from senslopedb.site_column_sim_nums
                where name = '%s' """ % (name.lower())
        
            a = cur.execute(query)
            if a:
                out = cur.fetchall()
                if (sim_num == out[0][0]):
                    print ">> Number already in database", name, out[0][0]
                    return
                                    
                break
            else:
                print '>> Number not in database', sim_num
                return
                break
        except MySQLdb.OperationalError:
            print '1.',
            raise KeyboardInterrupt
    
    query = """INSERT INTO senslopedb.site_column_sim_nums
                (name,sim_num,date_activated)
                VALUES ('%s','%s','%s')""" %(name.lower(),sim_num,date_activated)

    dbio.commitToDb(query, 'updateSimNumTable')

def checkNameOfNumber(number):
    db, cur = dbio.SenslopeDBConnect('local')
    
    while True:
        try:
            query = """select name from senslopedb.site_column_sim_nums
                where sim_num like '%s' """ % (number)
                
            a = cur.execute(query)
            if a:
                out = cur.fetchall()
                return out[0][0]                    
            else:
                print '>> Number not in database', number
                return ''
        except MySQLdb.OperationalError:
        # except KeyboardInterrupt:
            print '4.',        
            time.sleep(2)
            
    db.close()

def twoscomp(hexstr):
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

def ProcTwoAccelColData(msg,sender,txtdatetime):
    
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
    colid = msgsplit[0] # column id
        
    
    if len(msgsplit) != 4:
        print 'wrong data format'
        # print msg
        return

    if len(colid) != 5:
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
    # timestamp = "20"+ts[0:2]+"-"+ts[2:4]+"-"+ts[4:6]+" "+ts[6:8]+":"+ts[8:10]+":00"
    #print '>>',
    #print timestamp
    #timestamp = dt.strptime(txtdatetime,'%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
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
 # PARTITION the message into n characters
    if dtype == 'Y' or dtype == 'X':
       n = 15
       # PARTITION the message into n characters
       sd = [datastr[i:i+n] for i in range(0,len(datastr),n)]
    elif dtype == 'B':
        # do parsing for datatype 'B' (SOMS RAW)
        outl = SSP.somsparser(msg,1,10,0)       
        for piece in outl:
            print piece
    elif dtype == 'C':
        # do parsing for datatype 'C' (SOMS CALIB/NORMALIZED)
        outl = SSP.somsparser(msg,2,7,0)
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
                xd = twoscomp(piece[4:7])
                yd = twoscomp(piece[7:10])
                zd = twoscomp(piece[10:13])
                bd = (int(piece[13:15],16)+200)/100.0
                line = [colid,timestamp,ID,msgID,xd,yd,zd,bd]
                print line
                outl.append(line)
            except ValueError:
                print ">> Value Error detected.", piece,
                print "Piece of data to be ignored"
                    
    return outl

def WriteTwoAccelDataToDb(dlist,msgtime):
    query = """INSERT IGNORE INTO %s (timestamp,id,msgid,xvalue,yvalue,zvalue,batt) VALUES """ % str(dlist[0][0].lower())
    
    dbio.createTable(dlist[0][0], "sensor v2")
    for item in dlist:
        timetowrite = str(item[1])
        query = query + """('%s',%s,%s,%s,%s,%s,%s),""" % (timetowrite,str(item[2]),str(item[3]),str(item[4]),str(item[5]),str(item[6]),str(item[7]))

    query = query[:-1]
    # print len(query)
    
    dbio.commitToDb(query, 'WriteTwoAccelDataToDb')
   
def WriteSomsDataToDb(dlist,msgtime):
    query = """INSERT IGNORE INTO %s (timestamp,id,msgid,mval1,mval2) VALUES """ % str(dlist[0][0].lower())
    
    print "site_name", str(dlist[0][0])
    dbio.createTable(str(dlist[0][0]), "soms")
    for item in dlist:            
        timetowrite = str(item[1])
        query = query + """('%s',%s,%s,%s,%s),""" % (timetowrite,str(item[2]),str(item[3]),str(item[4]),str(item[5]))

    query = query[:-1]
    query = query.replace("nan","NULL")
    
    dbio.commitToDb(query, 'WriteSomsDataToDb')
    
def PreProcessColumnV1(data):
    data = data.replace("DUE","")
    data = data.replace(",","*")
    data = data.replace("/","")
    data = data[:-2]
    return data
    
def ProcessColumn(line,txtdatetime,sender):
    msgtable = line[0:4]
    print 'SITE: ' + msgtable
    ##msgdata = line[5:len(line)-11] #data is 6th char, last 10 char are date
    msgdata = (line.split('*'))[1]
    print 'raw data: ' + msgdata
    #getting date and time
    #msgdatetime = line[-10:]
    msgdatetime = (line.split('*'))[2][:10]
    print 'date & time: ' + msgdatetime

    # col_list = cfg.get("Misc","AdjustColumnTimeOf").split(',')
    # if msgtable in col_list:
    #     msgdatetime = txtdatetime
    #     print "date & time adjusted " + msgdatetime
    # else:
    #     msgdatetime = dt.strptime(msgdatetime,'%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
    #     print 'date & time no change'
        
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
        
    updateSimNumTable(msgtable,sender,msgdatetime[:10])
        
    query = query = """INSERT IGNORE INTO %s (timestamp,id,xvalue,yvalue,zvalue,mvalue) VALUES """ % (str(msgtable.lower()))
    
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
            
            query = query + """('%s',%s,%s,%s,%s,%s),""" % (str(msgdatetime),str(node_id),str(valueX),str(valueY),str(valueZ),str(valueF))
            
            print "%s\t%s\t%s\t%s\t%s" % (str(node_id),str(valueX),str(valueY),str(valueZ),str(valueF))
            
        query = query[:-1]
        
        # print query

        if i!=0:
            dbio.createTable(str(msgtable), "sensor v1")
            dbio.commitToDb(query, 'ProcessColumn')
                
    except KeyboardInterrupt:
        print '\n>>Error: Unknown'
        raise KeyboardInterrupt
        return
    except ValueError:
        print '\n>>Error: Unknown'
        return

def ProcessPiezometer(line,sender):    
    #msg = message
    print 'Piezometer data: ' + line
    try:
    #PUGBPZ*13173214*1511091800 
        linesplit = line.split('*')
        msgname = linesplit[0].lower()
        print 'msg_name: '+msgname        
        data = linesplit[1]
        msgid = int(('0x'+data[:2]), 16)
        p1 = int(('0x'+data[2:4]), 16)*100
        p2 = int(('0x'+data[4:6]), 16)
        p3 = int(('0x'+data[6:8]), 16)*.01
        piezodata = p1+p2+p3
        
        t1 = int(('0x'+data[8:10]), 16)
        t2 = int(('0x'+data[10:12]), 16)*.01
        tempdata = t1+t2
        try:
            txtdatetime = dt.strptime(linesplit[2],'%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
        except ValueError:
            txtdatetime = dt.strptime(linesplit[2],'%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
            
    except IndexError and AttributeError:
        print '\n>> Error: Piezometer message format is not recognized'
        print line
        return
    except ValueError:    
        print '>> Error: Possible conversion mismatch ' + line
        return      

        # try:
    dbio.createTable(str(msgname), "piezo")
    try:
      query = """INSERT INTO %s(timestamp, name, msgid, freq, temp ) VALUES ('%s','%s', %s, %s, %s )""" %(msgname,txtdatetime,msgname, str(msgid), str(piezodata), str(tempdata))
        
        # print query
    except ValueError:
        print '>> Error writing query string.', 
        return
    
    dbio.commitToDb(query, 'ProcessPiezometer')
        
    print 'End of Process Piezometer data'

def ProcessEarthquake(msg):
    line = msg.data
    print "Processing earthquake data"
    print msg.data

    dbio.createTable('earthquake', 'earthquake')

    #find date
    if re.search("\d{1,2}\w+201[6789]",msg.data):
        datestr = re.search("\d{1,2}\w+201[6789]",msg.data).group(0)
        print datestr
        try:
            datestr = dt.strptime(datestr,"%d%B%Y").strftime("%Y-%m-%d")
        except:
            print ">> Error in datetime conversion", datestr
            return False
    else:
        print ">> No date string recognized"
        return False

    #find time
    if re.search("\d{1,2}:\d{1,2}[AP]M",msg.data):
        timestr = re.search("\d{1,2}:\d{1,2}[AP]M",msg.data).group(0)
        try:
            timestr = dt.strptime(timestr,"%I:%M%p").strftime("%H:%M:00")
        except:
            print ">> Error in datetime conversion", timestr
            return False
    else:
        print ">> No time string recognized"
        return False

    datetimestr = datestr + ' ' + timestr
    
    #find magnitude
    if re.search("(?<=M[SB]\=)\d+\.\d+(?= )",msg.data):
        magstr = re.search("(?<=M[SB]\=)\d+\.\d+(?= )",msg.data).group(0)
    else:
        print ">> No magnitude string recognized"
        magstr = 'NULL'

    #find depth
    if re.search("(?<=D\=)\d+(?=K*M)",msg.data):
        depthstr = re.search("(?<=D\=)\d+(?=K*M)",msg.data).group(0)
    else:
        print ">> No depth string recognized"
        depthstr = 'NULL'

    #find latitude
    if re.search("\d+\.\d+(?=N)",msg.data):
        latstr = re.search("\d+\.\d+(?=N)",msg.data).group(0)
    else:
        print ">> No latitude string recognized"
        latstr = 'NULL'

    #find longitude
    if re.search("\d+\.\d+(?=E)",msg.data):
        longstr = re.search("\d+\.\d+(?=E)",msg.data).group(0)
    else:
        print ">> No longitude string recognized"
        longstr = 'NULL'

    #find epicenter distance
    if re.search("(?<=OR )\d+(?=KM)",msg.data):
        diststr = re.search("(?<=OR )\d+(?=KM)",msg.data).group(0)
    else:
        print ">> No distance string recognized"
        diststr = 'NULL'

    # find heading
    if re.search("[NS]\d+[EW]",msg.data):
        headstr = re.search("[NS]\d+[EW]",msg.data).group(0)
    else:
        print ">> No heading string recognized"
        headstr = 'NULL'

    # find Municipality
    if re.search("(?<=OF )[A-Z ]+(?= \()",msg.data):
        munistr = re.search("(?<=OF )[A-Z ]+(?= \()",msg.data).group(0)
    else:
        print ">> No municipality string recognized"
        munistr = 'NULL'

    # find province
    if re.search("(?<=\()[A-Z ]+(?=\))",msg.data):
        provistr = re.search("(?<=\()[A-Z ]+(?=\))",msg.data).group(0)
    else:
        print ">> No province string recognized"
        provistr = 'NULL'

    # find issuer
    if re.search("(?<=\<)[A-Z]+(?=\>)",msg.data):
        issuerstr = re.search("(?<=\<)[A-Z]+(?=\>)",msg.data).group(0)
    else:
        print ">> No issuer string recognized"
        issuerstr = 'NULL'

    query = "INSERT INTO senslopedb.earthquake (timestamp, mag, depth, lat, longi, dist, heading, municipality, province, issuer) VALUES ('%s',%s,%s,%s,%s,%s,'%s','%s','%s','%s') ON DUPLICATE KEY UPDATE mag=mag, depth=depth, lat=lat, longi=longi, dist=dist, heading=heading, municipality=municipality, province=province, issuer=issuer;" % (datetimestr,magstr,depthstr,latstr,longstr,diststr,headstr,munistr,provistr,issuerstr)

    print query

    dbio.commitToDb(query, 'earthquake')

    subprocess.Popen(["python","/home/dynaslope/Desktop/updews-pycodes/Data Analysis/eq_alert_gen.py"])

    return True


def ProcessARQWeather(line,sender):
    
    #msg = message

    print 'ARQ Weather data: ' + line

    line = re.sub("(?<=\+) (?=\+)","NULL",line)

    try:
    # ARQ+1+3+4.143+4.128+0.0632+5.072+0.060+0000+13+28.1+75.0+55+150727/160058
        #table name
        linesplit = line.split('+')
       
        msgname = checkNameOfNumber(sender).lower()
        if msgname:
            print ">> Number registered as", msgname
            #updateSimNumTable(msgname,sender,txtdatetime[:10])
            msgname = msgname + 'w'
        # else:
            # print ">> New number", sender
            # msgname = ''
            
        r15m = int(linesplit[1])*0.5
        r24h = int(linesplit[2])*0.5
        batv1 = linesplit[3]
        batv2 = linesplit[4]
        current = linesplit[5]
        boostv1 = linesplit[6]    
        boostv2 = linesplit[7]
        charge = linesplit[8]
        csq = linesplit[9]
        if csq=='':
            csq = '0'
        temp = linesplit[10]
        hum = linesplit[11]
        flashp = linesplit[12]
        txtdatetime = dt.strptime(linesplit[13],'%y%m%d/%H%M%S').strftime('%Y-%m-%d %H:%M:00')
        
        # print str(r15m),str(r24h),batv1, batv2, current, boostv1, boostv2, charge, csq, temp, hum, flashp,txtdatetime 

        
    except IndexError and AttributeError:
        print '\n>> Error: Rain message format is not recognized'
        print line
        return
    except ValueError:    
        print '>> Error: Possible conversion mismatch ' + line
        return
        
    if msgname:
        dbio.createTable(str(msgname), "arqweather")
    else:
        print ">> Error: Number does not have station name yet"
        return

    try:
        query = """INSERT INTO %s (timestamp,name,r15m, r24h, batv1, batv2, cur, boostv1, boostv2, charge, csq, temp, hum, flashp) 
        VALUES ('%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" % (msgname,txtdatetime,msgname,r15m, r24h, batv1, batv2, current, boostv1, boostv2, charge, csq, temp, hum, flashp)
        # print query
    except ValueError:
        print '>> Error writing query string.', 
        return

    dbio.commitToDb(query, 'ProcessARQWeather')
           
    print 'End of Process ARQ weather data'
    
def ProcessRain(line,sender):
    
    #msg = message
    line = re.sub("[^A-Z0-9,\/:\.\-]","",line)

    print 'Weather data: ' + line
    
    if len(line.split(',')) > 9:
        line = re.sub(",(?=$)","",line)
    line = re.sub("(?<=,)(?=(,|$))","NULL",line)
    line = re.sub("(?<=,)NULL(?=,)","0.0",line)
    print line

    try:
    
        msgtable = line.split(",")[0]
        msgdatetime = re.search("\d{02}\/\d{02}\/\d{02},\d{02}:\d{02}:\d{02}",line).group(0)

        txtdatetime = dt.strptime(msgdatetime,'%m/%d/%y,%H:%M:%S')
        # temporary adjust (wrong set values)
        if msgtable=="PUGW":
            txtdatetime = txtdatetime + td(days=1) # add one day
        elif msgtable=="PLAW":
            txtdatetime = txtdatetime + td(days=1)
        

        txtdatetime = txtdatetime.strftime('%Y-%m-%d %H:%M:00')
        
        # data = items.group(3)
        data = line.split(",",3)[3]

        # if msgtable in ('MAGW'):
        #     print '>> Adjusting rain value',
        #     rain_val_str = data.split(',')[3]
        #     rain_val_adj = float(rain_val_str) - 0.254
        #     data = data.replace(rain_val_str,str(rain_val_adj))
        #     print 'data adj >>', data

        
    except IndexError and AttributeError:
        print '\n>> Error: Rain message format is not recognized'
        print line
        return
    except KeyboardInterrupt:
        print '\n>>Error: Weather message format unknown ' + line
        return
        
    updateSimNumTable(msgtable,sender,txtdatetime[:10])

    dbio.createTable(str(msgtable),"weather")

    try:
        query = """INSERT INTO %s (timestamp,name,temp,wspd,wdir,rain,batt,csq) VALUES ('%s','%s',%s)""" %(msgtable.lower(),txtdatetime,msgtable,data)
            
    except:
        print '>> Error writing weather data to database. ' +  line
        return

    try:
        dbio.commitToDb(query, 'ProcesRain')
    except MySQLdb.ProgrammingError:
        print query[:-2]
        dbio.commitToDb(query[:-2]+')', 'ProcessRain')
        
    print 'End of Process weather data'

def ProcessStats(line,txtdatetime):

    print 'Site status: ' + line
    
    try:
        msgtable = "stats"
        items = re.match(r'(\w{4})[-](\d{1,2}[.]\d{02}),(\d{01}),(\d{1,2})/(\d{1,2}),#(\d),(\d),(\d{1,2}),(\d)[*](\d{10})',line)
        
        site = items.group(1)
        voltage = items.group(2)
        chan = items.group(3)
        att = items.group(4)
        retVal = items.group(5)
        msgs = items.group(6)
        sim = items.group(7)
        csq = items.group(8)
        sd = items.group(9)

        #getting date and time
        msgdatetime = line[-10:]
        print 'date & time: ' + msgdatetime

        col_list = cfg.get("Misc","AdjustColumnTimeOf").split(',')
        if site in col_list:
            msgdatetime = txtdatetime
            print "date & time adjusted " + msgdatetime
        else:
            print 'date & time no change'


    except IndexError and AttributeError:
        print '\n>> Error: Status message format is not recognized'
        print line
        return
    except:
        print '\n>>Error: Status message format unknown ' + line
        return

    dbio.createTable(str(msgtable),"stats")
        
    try:
        query = """INSERT INTO %s (timestamp,site,voltage,chan,att,retVal,msgs,sim,csq,sd)
        VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""" %(str(msgtable),str(msgdatetime),str(site),str(voltage),str(chan),str(att),str(retVal),str(msgs),str(sim),str(csq),str(sd))
            
    except:
        print '>> Error writing status data to database. ' +  line
        return

    
    dbio.commitToDb(query, 'ProcessStats')
        
    print 'End of Process status data'

def CheckMessageSource(msg):
    c = cfg.config()
    if dbio.checkNumberIfExists(msg.simnum,'community'):
        smsmsg = "From: %s %s of %s\n" % (identity[0][1],identity[0][0],identity[0][2])
        smsmsg += msg.data
        server.WriteOutboxMessageToDb(smsmsg,c.smsalert.communitynum)
        return
    elif dbio.checkNumberIfExists(msg.simnum,'dewsl'):
        print ">> From senslope staff"
        return

    name = dbio.checkNumberIfExists(msg.simnum,'sensor')    
    if name:
        print ">> From sensor", name[0][0]
    else:
        print "From unknown number ", msg.simnum

def ProcessAllMessages(allmsgs,network):
    c = cfg.config()
    read_success_list = []
    read_fail_list = []
    
    while allmsgs:
        isMsgProcSuccess = True
        print '\n\n*******************************************************'
        #gets per text message
        msg = allmsgs.pop(0)
        msg.data = msg.data.upper()
                     
        msgname = checkNameOfNumber(msg.simnum)
        ##### Added for V1 sensors removes unnecessary characters pls see function PreProcessColumnV1(data)
        if re.search("[A-Z]{4}DUE\*[A-F0-9]+\*\d+T?$",msg.data):
           msg.data = PreProcessColumnV1(msg.data)
           ProcessColumn(msg.data,msg.dt,msg.simnum)
        elif re.search("(RO*U*TI*N*E )|(EVE*NT )", msg.data.upper()):
            try:
                gm = gndmeas.getGndMeas(msg.data)
                RecordGroundMeasurements(gm)
                server.WriteOutboxMessageToDb("READ-SUCCESS: \n" + msg.data,c.smsalert.communitynum)
                server.WriteOutboxMessageToDb(c.reply.successen, msg.simnum)
            except ValueError as e:
                print str(e)
                errortype = re.search("(WEATHER|DATE|TIME|GROUND MEASUREMENTS|NAME)", str(e).upper()).group(0)
                print ">> Error in manual ground measurement SMS", errortype

                server.WriteOutboxMessageToDb("READ-FAIL: (%s)\n%s" % (errortype,msg.data),c.smsalert.communitynum)
                server.WriteOutboxMessageToDb(str(e), msg.simnum)
            except:
                server.WriteOutboxMessageToDb("READ-FAIL: (Unhandled) \n" + msg.data,c.smsalert.communitynum)
              
        elif re.search("^[A-Z]{4,5}\*[xyabcXYABC]\*[A-F0-9]+\*[0-9]+T?$",msg.data):
            try:
                dlist = ProcTwoAccelColData(msg.data,msg.simnum,msg.dt)
                if dlist:
                    if len(dlist[0][0]) == 6:
                        WriteSomsDataToDb(dlist,msg.dt)
                    else:
                        WriteTwoAccelDataToDb(dlist,msg.dt)
            except IndexError:
                print "\n\n>> Error: Possible data type error"
                print msg.data
        elif re.search("[A-Z]{4}\*[A-F0-9]+\*[0-9]+$",msg.data):
            #ProcessColumn(msg.data)
            ProcessColumn(msg.data,msg.dt,msg.simnum)
        #check if message is from rain gauge
        # elif re.search("^\w{4},[\d\/:,]+,[\d,\.]+$",msg.data):
        elif re.search("^\w{4},[\d\/:,]+",msg.data):
            ProcessRain(msg.data,msg.simnum)
        elif re.search(r'(\w{4})[-](\d{1,2}[.]\d{02}),(\d{01}),(\d{1,2})/(\d{1,2}),#(\d),(\d),(\d{1,2}),(\d)[*](\d{10})',msg.data):
            ProcessStats(msg.data,msg.dt)
        elif re.search("ARQ\+[0-9\.\+/\- ]+$",msg.data):
            ProcessARQWeather(msg.data,msg.simnum)
        elif msg.data[4:7] == "PZ*":
            ProcessPiezometer(msg.data, msg.simnum)
        elif msg.data.split('*')[0] == 'COORDINATOR' or msg.data.split('*')[0] == 'GATEWAY':
            isMsgProcSuccess = ProcessCoordinatorMsg(msg.data, msg.simnum)
        elif re.search("EQINFO",msg.data):
            isMsgProcSuccess = ProcessEarthquake(msg)
        elif re.search("^PSRI ",msg.data):
            isMsgProcSuccess = qsi.ProcessServerInfoRequest(msg)            
        else:
            print '>> Unrecognized message format: '
            print 'NUM: ' , msg.simnum
            print 'MSG: ' , msg.data
            CheckMessageSource(msg)            
            isMsgProcSuccess = False
            
        if isMsgProcSuccess:
            read_success_list.append(msg.num)
        else:
            read_fail_list.append(msg.num)
        
    return read_success_list, read_fail_list
    
def RecordGroundMeasurements(gnd_meas):
    # print gnd_meas
    
    dbio.createTable("gndmeas","gndmeas")
    
    query = "INSERT INTO gndmeas (timestamp, meas_type, site_id, observer_name, crack_id, meas, weather) VALUES " + gnd_meas
    query += "ON DUPLICATE KEY UPDATE meas = values(meas)"
    
    # print query
    
    dbio.commitToDb(query, 'RecordGroundMeasurements')

def RecordManualWeather(mw_text):
    # print gnd_meas
    
    dbio.createTable("manualweather","manualweather")
    
    query = "INSERT IGNORE INTO manualweather (timestamp, meas_type, site_id, observer_name, weatherdesc) VALUES " + mw_text
    
    dbio.commitToDb(query, 'RecordManualWeather')
        
def ProcessCoordinatorMsg(coordsms, num):

    print ">> Coordinator message received"
    print coordsms
    
    dbio.createTable("coordrssi","coordrssi")

    coordsms = re.sub("(?<=,)(?=(,|$))","NULL",coordsms)
    
    try:
        datafield = coordsms.split('*')[1]
        timefield = coordsms.split('*')[2]
        timestamp = dt.strptime(timefield,"%y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        
        smstype = datafield.split(',')[0]
        # process rssi parameters
        if smstype == "RSSI":
            site_name = datafield.split(',')[1]
            rssi_string = datafield.split(',',2)[2]
            print rssi_string
            # format is
            # <router name>,<rssi value>,...
            query = "INSERT IGNORE INTO coordrssi (timestamp, site_name, router_name, rssi_val) VALUES ("
            tuples = re.findall("[A-Z]+,\d+",rssi_string)
            for item in tuples:
                query += "'" + timestamp + "',"
                query += "'" + site_name + "',"
                query += "'" + item.split(',')[0] + "',"
                query += item.split(',')[1] + "),("
            
            query = query[:-2]

            print query
            
            dbio.commitToDb(query, 'ProcessCoordinatorMsg')

            return True
        else:
            print ">> Processing coordinator weather"
    except IndexError:
        print "IndexError: list index out of range"
        return False
    except:
        print ">> Unknown Error", coordsms
        return False

# for test codes    
def test():
    return

if __name__ == "__main__":
    test()