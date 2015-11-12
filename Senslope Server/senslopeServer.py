import os,time,serial,re,sys
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as td
import winsound
import emailer
global gsm_network
global anomalysave
import senslopedbio
#---------------------------------------------------------------------------------------------------------------------------

#class message:
#    def __init__(self,num,data,sender,):
#        self.num = ''
#        self.sender = ''
#        self.data = ''
#        self.dt = ''
        
def gsmflush():
    """Removes any pending inputs from the GSM modem and checks if it is alive."""
    try:
        gsm.flushInput()
        gsm.flushOutput()
        ghost = gsm.read(gsm.inWaiting())
        stat = gsmcmd('\x1a\rAT\r')    
        while('E' in stat):
            gsm.flushInput()
            gsm.flushOutput()
            ghost = gsm.read(gsm.inWaiting())
            stat = gsmcmd('\x1a\rAT\r')
        if ConsoleOutput:
            print '>> Flushing GSM buffer:', ghost
            print '>> Modem status: ', stat
    except serial.SerialException:
        print "NO SERIAL COMMUNICATION (gsmflush)"
        RunSenslopeServer(gsm_network)
    
def gsmcmd(cmd):
    """
    Sends a command 'cmd' to GSM Module
    Returns the reply of the module
    Usage: str = gsmcmd()
    """
    try:
        gsm.flushInput()
        gsm.flushOutput()
        a = ''
        now = time.time()
        gsm.write(cmd+'\r\n')
        while a.find('OK')<0 and time.time()<now+30:
                #print cmd
                #gsm.write(cmd+'\r\n')
                a += gsm.read(gsm.inWaiting())
                #a += gsm.read()
                #print '+' + a
                #print a
                time.sleep(0.5)
        if time.time()>now+30:
                a = '>> Error: GSM Unresponsive'
        return a
    except serial.SerialException:
        print "NO SERIAL COMMUNICATION (gsmcmd)"
        RunSenslopeServer(gsm_network)
		
def sendMsg(alert_msg, number):
    """
    Sends a command 'cmd' to GSM Module
    Returns the reply of the module
    Usage: str = gsmcmd()
    """
    try: 
        gsm.flushInput()
        gsm.flushOutput()
        a = ''
        now = time.time()
        preamble = "AT+CMGS=\""+number+"\""
        print "\nMSG:"+alert_msg
        print "NUM:"+number
        gsm.write(preamble+"\r")
        while a.find('>')<0 and time.time()<now+30:
                #print cmd
                #gsm.write(cmd+'\r\n')
                a += gsm.read(gsm.inWaiting())
                #a += gsm.read()
                #print '+' + a
                #print a
                time.sleep(0.5)

        if time.time()>now+30:  
                a = '>> Error: GSM Unresponsive'
        
        gsm.flushInput()
        gsm.flushOutput()
        a = ''
        now = time.time()
        gsm.write(alert_msg+chr(26))
        while a.find('OK\r\n')<0 and time.time()<now+30:
                #print cmd
                #gsm.write(cmd+'\r\n')
                a += gsm.read(gsm.inWaiting())
                #a += gsm.read()
                #print '+' + a
                #print a
                time.sleep(0.5)
        if time.time()>now+30:
                a = '>> Error: GSM Unresponsive'
        return a
    except serial.SerialException:
        print "NO SERIAL COMMUNICATION (sendmsg)"
        RunSenslopeServer(gsm_network)	

def countmsg():
    global anomalysave
    """
    Gets the # of SMS messages stored in GSM modem.
    Usage: c = countmsg()
    """
    if FileInput:
        return 1
    anomalysave = ''
    b = gsmcmd('AT+CPMS?')
    anomalysave = b
    try:
        c = int( b.split(',')[1] )
        #print '>>>> ', c
        return c
    except IndexError:
        print 'count_msg b = ',b
        if b:
            return 0
        else:
            return -1
        ##if GSM sent blank data maybe GSM is inactive
    except ValueError:
        print '>> ValueError:'
        print b
        return -2
#---------------------------------------------------------------------------------------------------------------------------

def SenslopeDBConnect():
    while True:
        try:
            db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb, db = Namedb)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
            print '6.',
            time.sleep(2)

def InitLocalDB():
    db, cur = SenslopeDBConnect()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    db.close()
   
def createTable(table_name):
    db, cur = SenslopeDBConnect()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, xvalue int, yvalue int, zvalue int, mvalue int, PRIMARY KEY (timestamp, id))" %table_name)
    db.close()

def createTableTwoAccelSensor(table_name):
    db, cur = SenslopeDBConnect()
    try:
        cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
        cur.execute("USE %s"%Namedb)
        cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, msgid smallint, xvalue int, yvalue int, zvalue int, batt double, PRIMARY KEY (timestamp, id, msgid))" %table_name)
    except MySQLdb.ProgrammingError:
        print ">> Error: creating table for table name '", table_name, "'"
        return
    db.close()

def createTableWeather(table_name):
    db, cur = SenslopeDBConnect()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, name char(4), temp double,wspd int, wdir int,rain double,batt double, csq int, PRIMARY KEY (timestamp, name))" %table_name)
    db.close()
    
def createTableARQWeather(table_name):
    db, cur = SenslopeDBConnect()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, name char(6), r15m double, r24h double, batv1 double, batv2 double, cur double, boostv1 double, boostv2 double, charge int, csq int, temp double, hum double, flashp int, PRIMARY KEY (timestamp, name))" %table_name)
    db.close()
    
def createTablePiezodata(table_name):
    db, cur = SenslopeDBConnect()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, name char(7), msgid int , freq double,PRIMARY KEY (timestamp, name))"%table_name)
    db.close()   
    
def createTableStats(table_name):
    db, cur = SenslopeDBConnect()
    #Namedb = "stats"
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, site char(4), voltage double, chan int, att int, retVal int, msgs int, sim int, csq int, sd int, PRIMARY KEY (timestamp, site))" %table_name)
    db.close()
    
def createTableSOMS(table_name):
    db, cur = SenslopeDBConnect()
    #Namedb = "stats"
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %Namedb)
    cur.execute("USE %s"%Namedb)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, msgid smallint, mval1 int, mval2 int, PRIMARY KEY (timestamp, id, msgid))" %table_name)
    db.close()
    
def updateSimNumTable(name,sim_num,date_activated):
    db, cur = SenslopeDBConnect()
    
    while True:
        try:
            query = """select sim_num from senslopedb.site_column_sim_nums
                where name = '%s' """ % (name)
        
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
            
    while True:
        try:
            query = """INSERT INTO senslopedb.site_column_sim_nums
                (name,sim_num,date_activated)
                VALUES ('%s','%s','%s')""" %(name,sim_num,date_activated)
        
            a = cur.execute(query)
            if a:
                db.commit()
                print ">> Number written to database", name, sim_num
                break
            else:
                print '>> Warning: Query has no result set (updateSimNumTable)'
                time.sleep(5)
        except MySQLdb.OperationalError:
            print '2.',
            raise KeyboardInterrupt
        except MySQLdb.IntegrityError:
            print '>> Duplicate entry', name, sim_num
            break
        except MySQLdb.ProgrammingError:
            print ">> Cannot write entry"
            break
            
    db.close()

def updateLastMsgReceivedTable(txtdatetime,name,sim_num,msg):
    db, cur = SenslopeDBConnect()
    
    # print ">> Saveing last meesage receievd"
    while True:
        try:
            query = """insert into senslopedb.last_msg_received
                (timestamp,name,sim_num,last_msg)
                values ('%s','%s','%s','%s')
                on DUPLICATE key update
                timestamp = '%s',
                sim_num = '%s',
                last_msg = '%s'""" %(txtdatetime,name,sim_num,msg,txtdatetime,sim_num,msg)
        
            a = cur.execute(query)
            if a:
                db.commit()
                print ">> Message written to database", name, sim_num
                break
            else:
                print '>> Warning: Query has no result set (updateLastMsgReceivedTable)'
                
                time.sleep(2)
                break
        except MySQLdb.OperationalError:
        # except IndexError:
            print '3.',
            raise KeyboardInterrupt
        except MySQLdb.ProgrammingError:
            print ">> Cannot write entry"
            break
            
            
    db.close()
       
def checkNameOfNumber(number):
    db, cur = SenslopeDBConnect()
    
    while True:
        try:
            query = """select name from senslopedb.site_column_sim_nums
                where sim_num = '%s' """ % (number)
                
            a = cur.execute(query)
            if a:
                out = cur.fetchall()
                return out[0][0]                    
            else:
                print '>> Number not in database'
                return ''
        except MySQLdb.OperationalError:
        # except KeyboardInterrupt:
            print '4.',        
            time.sleep(2)
            
    db.close()
 
def timeToSendAlertMessage(ref_minute):
    current_minute = dt.now().minute
    if current_minute % AlertReportInterval != 0:
        return 0,ref_minute
    elif ref_minute == current_minute:
        return 0,ref_minute
    else:
        return 1,current_minute

# def WriteLinesToDB(lines):
    # if not WriteToDB:
        # print "Writing to database not enabled"
        # return

    # for line in lines:
        # db, cur = SenslopeDBConnect()

        # [msgtable,msgdatetime,nid,xval,yval,zval,mval] = line

        

        # try:
            # query = """INSERT INTO %s
                # (timestamp,id,xvalue,yvalue,zvalue,mvalue)
                # VALUES ('%s',%s,%s,%s,%s,%s)""" %(msgtable,msgdatetime,str(nid),str(xval),str(yval),str(zval),str(mval))

            # a = cur.execute(query)
            # if a:
                # db.commit()
            # else:
                # print '>> Warning: Query has no result set (WriteLinesToDB)'
                # time.sleep(2)
        # except KeyError:
            # print '>> Error: Writing to database'
        # except MySQLdb.IntegrityError:
            # print '>> Warning: Duplicate entry detected'
            
        # db.close()
            
        # print "%s\t%s\t%s\t%s\t%s" % (str(nid),str(xval),str(yval),str(zval),str(mval))

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
    msgsplit = msg.split('*')
    colid = msgsplit[0] # column id
    
    if len(msgsplit) != 4:
        print 'wrong data format'
        print msg
        return

    if len(colid) != 5:
        print 'wrong master name'
        return

    print '\n\n*******************************************************'
    print msg

    dtype = msgsplit[1]
   
    datastr = msgsplit[2]
    
    ts = msgsplit[3]
  
    if datastr == '':
        datastr = '000000000000000'
        print "Error: No parsed data in sms"
        return
   
    if len(ts) < 10:
       print 'Error in time value format: '
       return
       
    if sender == '639175940773':
        # timestamp = dt.strptime(txtdatetime,'%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
        colid = 'BAYTC'
        print '>> name change',
    else:
        timestamp = "20"+ts[0:2]+"-"+ts[2:4]+"-"+ts[4:6]+" "+ts[6:8]+":"+ts[8:10]+":00"
        #print '>>',
    #print timestamp
    #timestamp = dt.strptime(txtdatetime,'%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
    try:
        timestamp = dt.strptime(ts,'%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
    except ValueError:
        print "Error: wrong timestamp format", ts

    if dtype == 'x' or dtype == 'X':
       n = 15
       accel = 1
    elif dtype == 'y' or dtype == 'Y':
        n = 15  
        accel =2
    elif dtype == 'b':
        n = 12
        colid =  colid + 'M'
    # elif dtype == 'c':
        # n = 7
        # colid =  colid + 'W'    
    # elif dtype == 'w':
        # n = 26
    # elif dtype == 'W':
        # n = 23
    # elif dtype == 'z':
        # n = 8
    # elif dtype == 'Z':
        # n = 5
    # elif dtype == 'a':
        # n = 15
    # elif dtype == '0':
        
        # n = 15
        # datastr = '010000000000000'
    
    else:
        raise IndexError("Undefined data format " + dtype )
    
    outl = []
 
    # print datastr
    
    # updateSimNumTable(colid,sender,timestamp[:10])

    # PARTITION hte message into n characters
    sd = [datastr[i:i+n] for i in range(0,len(datastr),n)]
    
    # do parsing for different data types
    if dtype.upper() == 'X' or dtype.upper() =='Y':
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
    elif dtype.upper() == 'B' or dtype.upper() == 'C':
        for piece in sd:
            try:
                # print piece
                ID = int(piece[0:2],16)
                msgID = int(piece[2:4],16)
                m1 = twoscomp(piece[4:7])
                try:
                    m2 = twoscomp(piece[7:10])
                except:
                    m2 = 'NULL'
                line = [colid,timestamp,ID,msgID,m1,m2]
                print line
                outl.append(line)
            except ValueError:
                print ">> Value Error detected.", piece,
                print "Piece of data to be ignored"
    
                    
    return outl

def WriteTwoAccelDataToDb(dlist,msgtime):
    query = """INSERT IGNORE INTO %s (timestamp,id,msgid,xvalue,yvalue,zvalue,batt) VALUES """ % str(dlist[0][0])
    if WriteToDB:
        for item in dlist:
            db, cur = SenslopeDBConnect()
            createTableTwoAccelSensor(item[0])
            timetowrite = str(item[1])
            query = query + """('%s',%s,%s,%s,%s,%s,%s),""" % (timetowrite,str(item[2]),str(item[3]),str(item[4]),str(item[5]),str(item[6]),str(item[7]))

    query = query[:-1]
    try:
        # query = """INSERT INTO %s
            # (timestamp,id,msgid,xvalue,yvalue,zvalue,batt)
            # VALUES ('%s',%s,%s,%s,%s,%s,%s)""" %(str(item[0]),timetowrite,str(item[2]),str(item[3]),str(item[4]),str(item[5]),str(item[6]),str(item[7]))
        retry = 0
        while True:
            try:
                a = cur.execute(query)
                # db.commit()
                if a:
                    db.commit()
                    break
                else:
                    print '>> Warning: Query has no result set (WriteTwoAccelDataToDb)'
                    time.sleep(2)
                    break
            except MySQLdb.OperationalError:
                print '5.',
                #time.sleep(2)
                if retry > 10:
                    return
                else:
                    retry += 1
                    time.sleep(2)
            except MySQLdb.ProgrammingError:
                print ">> Unable to write to table '" + str(item[0]) + "'"
                return
                    
            
    except KeyError:
        print '>> Error: Writing to database'
    except MySQLdb.IntegrityError:
        print '>> Warning: Duplicate entry detected'
        
            
        db.close()
        
    #print "%s\t%s\t%s\t%s\t%s" % (str(node_id),str(valueX),str(valueY),str(valueZ),str(valueF))

def WriteSomsDataToDb(dlist,msgtime):
    query = """INSERT IGNORE INTO %s (timestamp,id,msgid,mval1,mval2) VALUES """ % str(dlist[0][0])
    if WriteToDB:
        for item in dlist:
            db, cur = SenslopeDBConnect()
            createTableSOMS(item[0])
            timetowrite = str(item[1])
            query = query + """('%s',%s,%s,%s,%s),""" % (timetowrite,str(item[2]),str(item[3]),str(item[4]),str(item[5]))

    query = query[:-1]
    #print query
    try:
        
        retry = 0
        while True:
            try:
                a = cur.execute(query)
                # db.commit()
                if a:
                    db.commit()
                    break
                else:
                    print '>> Warning: Query has no result set (WriteSomsDataToDb)'
                    time.sleep(2)
                    break
            except MySQLdb.OperationalError:
            #except IndexError:
                print '5.',
                #time.sleep(2)
                if retry > 10:
                    return
                else:
                    retry += 1
                    time.sleep(2)
    except KeyError:
        print '>> Error: Writing to database'
    except MySQLdb.IntegrityError:
        print '>> Warning: Duplicate entry detected'
        
            
        db.close()
        
    #print "%s\t%s\t%s\t%s\t%s" % (str(node_id),str(valueX),str(valueY),str(valueZ),str(valueF))   
    
def PreProcessColumnV1(data):
    data = data.replace("DUE","")
    data = data.replace(",","*")
    data = data.replace("/","")
    data = data[:-2]
    return data
    
def ProcessColumn(line,txtdatetime,sender):
    #msg = message
    
    #print 'line: ' + line
    msgtable = line[0:4]
    print '\n\n*******************************************************'
    print 'SITE: ' + msgtable
    ##msgdata = line[5:len(line)-11] #data is 6th char, last 10 char are date
    msgdata = (line.split('*'))[1]
    print 'raw data: ' + msgdata
    #getting date and time
    #msgdatetime = line[-10:]
    msgdatetime = (line.split('*'))[2][:10]
    print 'date & time: ' + msgdatetime

    col_list = cfg.get("Misc","AdjustColumnTimeOf").split(',')
    if msgtable in col_list:
        msgdatetime = txtdatetime
        print "date & time adjusted " + msgdatetime
    else:
        msgdatetime = dt.strptime(msgdatetime,'%y%m%d%H%M').strftime('%Y-%m-%d %H:%M:00')
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
        
    updateSimNumTable(msgtable,sender,msgdatetime[:10])
        
    query = query = """INSERT IGNORE INTO %s (timestamp,id,xvalue,yvalue,zvalue,mvalue) VALUES """ % (str(msgtable))
    
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

        if WriteToDB and i!=0:
            db, cur = SenslopeDBConnect()
            
            createTable(str(msgtable))

            try:
                
                a = cur.execute(query)
                if a:
                    db.commit()
                else:
                    print '>> Warning: Query has no result set (ProcessColumn)'
                    time.sleep(2)
                    
            except KeyError:
                print '>> Error: Writing to database'
            except MySQLdb.IntegrityError:
                print '>> Warning: Duplicate entry detected'
                
            db.close()
                
    except KeyboardInterrupt:
        print '\n>>Error: Unknown'
        raise KeyboardInterrupt
        return
    except ValueError:
        print '\n>>Error: Unknown'
        return

def ProcessPiezometer(line,sender):    
    #msg = message
    print '\n\n*******************************************************'   
    print 'Piezometer data: ' + line
    try:
    #PUGBPZ*13173214*1511091800 
        linesplit = line.split('*')
        msgname = linesplit[0]
        print 'msg_name: '+msgname        
        data = linesplit[1]
        siteid = int(data[:2])
        p1 = int(('0x'+data[2:4]), 16)*100
        p2 = int(('0x'+data[4:6]), 16)
        p3 = int(('0x'+data[6:]), 16)*.01
        piezodata = p1+p2+p3
        txtdatetime = dt.strptime(linesplit[2],'%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:00')
            
    except IndexError and AttributeError:
        print '\n>> Error: Piezometer message format is not recognized'
        print line
        return
    except ValueError:    
        print '>> Error: Possible conversion mismatch ' + line
        return      

        # try:
    if WriteToDB:
        db, cur = SenslopeDBConnect()        
        createTablePiezodata(str(msgname))
        try:
          query = """INSERT INTO %s(timestamp, name, msgid, freq ) VALUES ('%s','%s', %s, %s )""" %(msgname,txtdatetime,msgname, siteid, piezodata)
            
            # print query
        except ValueError:
            print '>> Error writing query string.', 
            return
        
        try:
            a = cur.execute(query)
            if a:
                db.commit()
            else:
                print '>> Query has no result set (ProcessPiezometer)'
                time.sleep(2)
        except KeyError:
            print '>> Error: Writing to database'
        except MySQLdb.IntegrityError:
            print '>> Warning: Duplicate entry detected'
        # except:
            # print '>> Unknown error in message data: ', sys.exc_info()[0], sys.exc_info()[1] 
        # db.close()
        #except:
        #    print '>> Error: Rain format corrupted',
    
    print 'End of Process Piezometer data'

        
def ProcessARQWeather(line,sender):
    
    #msg = message

    print '\n\n*******************************************************'   
    print 'ARQ Weather data: ' + line

    try:
    # ARQ+1+3+4.143+4.128+0.0632+5.072+0.060+0000+13+28.1+75.0+55+150727/160058
        #table name
        linesplit = line.split('+')
       
        msgname = checkNameOfNumber(sender)
        if msgname:
            print ">> Number registered as", msgname
            #updateSimNumTable(msgname,sender,txtdatetime[:10])
            msgname = msgname + 'W'
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
        
    

    # try:
    if WriteToDB:
        db, cur = SenslopeDBConnect()
        
        if msgname:
            createTableARQWeather(str(msgname))
        else:
            print ">> Error: Number does not have station name yet"
            return

        try:
            query = """INSERT INTO %s (timestamp,name,r15m, r24h, batv1, batv2, cur, boostv1, boostv2, charge, csq, temp, hum, flashp) VALUES ('%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" %(msgname,txtdatetime,msgname,r15m, r24h, batv1, batv2, current, boostv1, boostv2, charge, csq, temp, hum, flashp)
            # print query
        except ValueError:
            print '>> Error writing query string.', 
            return

        
        try:
            a = cur.execute(query)
            if a:
                db.commit()
            else:
                print '>> Query has no result set (ProcessARQWeather)'
                time.sleep(2)
        except KeyError:
            print '>> Error: Writing to database'
        except MySQLdb.IntegrityError:
            print '>> Warning: Duplicate entry detected'
        # except:
            # print '>> Unknown error in message data: ', sys.exc_info()[0], sys.exc_info()[1] 
        # db.close()
        #except:
        #    print '>> Error: Rain format corrupted',
    
    print 'End of Process ARQ weather data'
    
def ProcessRain(line,sender):
    
    #msg = message

    print '\n\n*******************************************************'   
    print 'Weather data: ' + line

    try:
    
        items = re.match(".*(\w{4})[, ](\d{02}\/\d{02}\/\d{02},\d{02}:\d{02}:\d{02})[,\*](\d{2}.\d,\d{1,3},\d{1,3},\d.\d{1,2},\d{1,2}.\d{1,2},\d{1,2}),*\*?.*",line)
        msgtable = items.group(1)
        msgdatetime = items.group(2)

        txtdatetime = dt.strptime(msgdatetime,'%m/%d/%y,%H:%M:%S')
        #temporary adjust (wrong set values)
        if msgtable=="PUGW":
            txtdatetime = txtdatetime + td(days=1) # add one day
        elif msgtable=="PLAW":
            txtdatetime = txtdatetime + td(days=1)

        txtdatetime = txtdatetime.strftime('%Y-%m-%d %H:%M:00')
        
        data = items.group(3)
        
    except IndexError and AttributeError:
        print '\n>> Error: Rain message format is not recognized'
        print line
        return
    except:
        print '\n>>Error: Weather message format unknown ' + line
        return
        
    updateSimNumTable(msgtable,sender,txtdatetime[:10])

    #try:
    if WriteToDB:
        db, cur = SenslopeDBConnect()
        
        createTableWeather(str(msgtable))

        try:
            query = """INSERT INTO %s (timestamp,name,temp,wspd,wdir,rain,batt,csq) VALUES ('%s','%s',%s)""" %(msgtable,txtdatetime,msgtable,data)
                
        except:
            print '>> Error writing weather data to database. ' +  line
            return

        
        try:
            a = cur.execute(query)
            if a:
                db.commit()
                print a
            else:
                print '>> Query has no resultset (ProcessRain)'
                time.sleep(2)
        except KeyError:
            print '>> Error: Writing to database'
        except MySQLdb.IntegrityError:
            print '>> Warning: Duplicate entry detected'
        except:
            print '>> Unknown error in message data: ', sys.exc_info()[0], sys.exc_info()[1] 
        db.close()
    #except:
    #    print '>> Error: Rain format corrupted',
    
    print 'End of Process weather data'

def ProcessStats(line,txtdatetime):

    print '\n\n*******************************************************'
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

    if WriteToDB:
        db, cur = SenslopeDBConnect()

        createTableStats(str(msgtable))
            
        try:
            query = """INSERT INTO %s (timestamp,site,voltage,chan,att,retVal,msgs,sim,csq,sd)
            VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""" %(str(msgtable),str(msgdatetime),str(site),str(voltage),str(chan),str(att),str(retVal),str(msgs),str(sim),str(csq),str(sd))
                
        except:
            print '>> Error writing status data to database. ' +  line
            return

        
        try:
            a = cur.execute(query)
            if a:
                db.commit()
            else:
                print '>> No 3'
                time.sleep(2)
        except KeyError:
            print '>> Error: Writing to database'
        except MySQLdb.IntegrityError:
            print '>> Warning: Duplicate entry detected'
        except:
            print '>> Unknown error in message data: ', sys.exc_info()[0], sys.exc_info()[1] 
        db.close()
    
    print 'End of Process status data'
    
def runBackup():
    print '>> Backing up Database..',
    try:
        query = 'mysqldump -h '+Hostdb+' -u '+Userdb+' -p'+\
                  Passdb+' '+Namedb+' > '+Namedb+'-backup.sql'
        os.system(query)
    except:
        print '>> Error backing up database'
    else:
        print 'done'

    print '>> Updating csv files..'
    extract_db()
    #format_data()
    #extract_rain()
    print 'done'

    return dt.today()

def RunSenslopeServer(network):
    global gsm_network
    global anomalysave
    gsm_network  = network
    minute_of_last_alert = dt.now().minute
    timetosend = 0
    email_flg = 0
    if network == "SUN":
        Port = cfg.getint('Serial', 'SunPort') - 1
    elif network == "GLOBE":
        Port = cfg.getint('Serial', 'GlobePort') - 1
    else:
        Port = cfg.getint('Serial', 'SmartPort') - 1

    if not FileInput:
        try:
            print 'Connecting to GSM modem at COM',
            gsm.port = Port
            gsm.baudrate = Baudrate
            gsm.timeout = Timeout
            gsm.open()
            #gsmflush()
            print Port+1
            print 'Switching to no-echo mode', gsmcmd('ATE0').strip('\r\n')
            print 'Switching to text mode', gsmcmd('AT+CMGF=1').rstrip('\r\n')
        
        except serial.SerialException:
            print ">> ERROR: Could not open COM %r!" % (Port+1)
            print '**NO COM PORT FOUND**'
            while True:
                if not FileInput:
                    gsm.close()
                    if (not email_flg):
                        sender = '1234dummymailer@gmail.com'
                        sender_password = '1234dummy'
                        receiver = 'ggilbertluis@gmail.com'
                        noserial_message = 'Please fix me'
                        if (gsm_network == 'GLOBE'):
                            subject = 'No Serial Email Notification (GLOBE SERVER)'
                        else:
                            subject = 'No Serial Email Notification (SMART SERVER)'
                            
                        emailer.sendmessage(sender,sender_password,receiver,sender,subject,noserial_message)
                        receiver = 'dynabeta@gmail.com'
                        emailer.sendmessage(sender,sender_password,receiver,sender,subject,noserial_message)
                        email_flg = 1
			
    # force backup
    #last_backup = runBackup()
    #last_backup = dt.today().day()
    print '**' + network + ' GSM server active**'
    print time.asctime()
    while True:
        m = countmsg()
        if m>0:
            if not FileInput:
                # make a list of all messages
                # remove the OK field
                # isolate each message by using +CMGL as delimiter
                # make sure start of string is not a legitimate message
                allmsgs = 'd' + gsmcmd('AT+CMGL="ALL"')
                allmsgs = allmsgs.replace("\r\nOK\r\n",'').split("+CMGL")[1:]
                if allmsgs:
                    temp = allmsgs.pop(0) #removes "=ALL"
                
            else:
                print '>> Reading from: ', InputFile
                f = open(InputFile,'r')
                allmsgs = f.readlines()
                #allmsgs = a.split(":")[1:]
                f.close()

            while allmsgs:
                if not FileInput:
                    #gets per text message
                    temp = allmsgs.pop(0)
                                 
                    if SaveToFile:
                        try:
                            mon = dt.now().strftime("-%Y-%B-")
                            f = open("D:\\Server Files\\Consolidated\\"+network+mon+'backup.txt','a')
                            f.write(temp)
                            f.close()
                        except:
                            print 'Warning: Cannot write to "backup.txt".'
                    temp = temp.replace('\n','').split("\r")
                    #msg = message
                    try:
                        sender = re.search(r'[0-9]{12}',temp[0]).group(0)
                    except:
                        print 'Sender unknown.'
                        sender = "UNK"
                    #msg.num = re.search(r': [0-9]{1,2},',temp[0]).group(0).strip(': ,')
                    txtnum = re.search(r': [0-9]{1,2},',temp[0]).group(0).strip(': ,')
                    try:
                        txtdatetimeStr = re.search(r'\d\d/\d\d/\d\d,\d\d:\d\d:\d\d',temp[0]).group(0)
                        txtdatetime = dt.strptime(txtdatetimeStr,'%y/%m/%d,%H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
                    except:
                        print "Error in date time conversion"
                    line = temp[1]
                        
                else:
                    temp = allmsgs.pop(0)
                    temp = temp.replace('\n','').split("\r")
                    line = temp[0]
                
                msgname = checkNameOfNumber(sender)
                ##### Added for V1 sensors removes unnecessary characters pls see function PreProcessColumnV1(data)
                if line.find("DUE*") >0:
                   line = PreProcessColumnV1(line)
                ####not sure where to put this function for trial
                
                if len(line.split("*")[0]) == 5:
                    try:
                        dlist = ProcTwoAccelColData(line,sender,txtdatetime)
                        #print dlist
                        if dlist:
                            if len(dlist[0][0]) == 6:
                                WriteSomsDataToDb(dlist,txtdatetime)
                            else:
                                WriteTwoAccelDataToDb(dlist,txtdatetime)
                    except IndexError:
                        print "\n\n>> Error: Possible data type error"
                        print line
                elif len(line)>4 and line[4] == '*':
                    #ProcessColumn(line)
                    ProcessColumn(line,txtdatetime,sender)
                #check if message is from rain gauge
                elif re.search("(\w{4})[, ](\d{02}\/\d{02}\/\d{02},\d{02}:\d{02}:\d{02})[,\*](-*\d{2}.\d,\d{1,3},\d{1,3},\d{1,2}.\d{1,2},\d.\d{1,2},\d{1,2}),*\*?",line):
                    ProcessRain(line,sender)
                elif re.search(r'(\w{4})[-](\d{1,2}[.]\d{02}),(\d{01}),(\d{1,2})/(\d{1,2}),#(\d),(\d),(\d{1,2}),(\d)[*](\d{10})',line):
                    ProcessStats(line,txtdatetime)
                elif line[:4] == "ARQ+":
                    ProcessARQWeather(line,sender)
                    
                #if message is from piezometer
                elif line[4:7] == "PZ*":
                    ProcessPiezometer(line, sender)
                else:
                    print '\n\n*******************************************************'
                    print '>> Unrecognized message format: '
                    print 'NUM: ' , sender
                    print 'MSG: ' , line
                    
                #msgname = checkNameOfNumber(sender)
                if msgname:
                    updateLastMsgReceivedTable(txtdatetime,msgname,sender,line)
                    
                    if SaveToFile:
                        dir = "D:\\Server Files\\Consolidated\\"+"\\Inbox"+"\\"+msgname
                        if not os.path.exists(dir):
                            os.makedirs(dir)
                        f = open(dir+'\\'+msgname+'-backup.txt','a')
                        f.write(txtdatetime+',')
                        f.write(line+'\n')
                        f.close()
                        
                else:
                    f = open("D:\\Server Files\\Consolidated\\"+"Unknown-sender.txt",'a')
                    f.write(txtdatetime+',')
                    f.write(sender+',')
                    f.write(line+'\n')
                    f.close()
                        
                if DeleteAfterRead and not FileInput:
                    print 'Deleting message...'
                    try:
                        gsmcmd('AT+CMGD='+txtnum).strip()
                        print 'OK'
                    except:
                        print 'Error deleting message: ', line

            if FileInput:
                break
            
            print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
            
        elif  m == 0:
            time.sleep(SleepPeriod)
            gsmflush()

            today = dt.today()
            
            if (today.minute % 10 == 0):
                if checkIfActive:
                    print today.strftime("\nServer active as of %A, %B %d, %Y, %X")
                checkIfActive = False
            
            else:
                checkIfActive = True
                if (today.minute % 10):
                    email_flg = 0;
                
        elif m == -1:
            print'GSM MODULE MAYBE INACTIVE' 
            while True:
                if not FileInput:
                    gsm.close()
                    if (not email_flg):
                        sender = '1234dummymailer@gmail.com'
                        sender_password = '1234dummy'
                        receiver = 'ggilbertluis@gmail.com'
                        nogsm_message = 'Please fix me'
                        if (gsm_network == 'GLOBE'):
                            subject = 'GSM INACTIVE Notification (GLOBE SERVER)'
                        else:
                            subject = 'GSM INACTIVE Notification (SMART SERVER)'
                            
                        emailer.sendmessage(sender,sender_password,receiver,sender,subject,nogsm_message)
                        receiver = 'dynabeta@gmail.com'
                        emailer.sendmessage(sender,sender_password,receiver,sender,subject,nogsm_message)
                        email_flg = 1
        elif m == -2:
            print 'Value error\nNow Saving to Anomaly text file'
            today = dt.today()
            nowdate = today.strftime("as  of %A, %B %d, %Y, %X")
            if gsm_network=='GLOBE':
                f = open("Anomaly-GlobeServer.txt",'a')
            else:
                f = open("Anomaly-SmartServer.txt",'a')            
            f.write('\ncount_msg return ='+ str(m) + '\n')
            f.write('\nanomaly data = '+ anomalysave)
            f.write('\nDate/time:'+ nowdate )
            f.close()
                        
        else:
            print 'Some other error\nNow Saving to Anomaly text file'
            today = dt.today()
            nowdate = today.strftime("as  of %A, %B %d, %Y, %X")
            if gsm_network=='GLOBE':
                f = open("Anomaly-GlobeServer.txt",'a')
            else:
                f = open("Anomaly-SmartServer.txt",'a') 
                
            f.write('\nSome other anomaly,\ncount_msg return ='+ str(m)+ '\n')
            f.write('\nanomaly data = '+ anomalysave)
            f.write('\nDate/time:'+ nowdate)
            f.close()
                        
        today = dt.today()
        if (today.minute % 30 == 0):
            if (not email_flg):
                sender = '1234dummymailer@gmail.com'
                sender_password = '1234dummy'
                receiver = 'ggilbertluis@gmail.com'
                if (gsm_network == 'GLOBE'):
                    subject = today.strftime("ACTIVE GLOBE SERVER Notification as  of %A, %B %d, %Y, %X")
                    active_message = '\nGood Day!\n\nYou received this email because GLOBE SERVER is still active!\nPlease pray for me so that I can be active as always!\n Thanks!\n\n- Globe Server\n'
                else:
                    subject = today.strftime("ACTIVE SMART SERVER Notification as  of %A, %B %d, %Y, %X")
                    active_message = '\nGood Day!\n\nYou received this email because SMART SERVER is still active!\nPlease pray for me so that I can be active as always!\n Thanks!\n\n-Smart Server\n'
                emailer.sendmessage(sender,sender_password,receiver,sender,subject,active_message)
                receiver = 'dynabeta@gmail.com'
                emailer.sendmessage(sender,sender_password,receiver,sender,subject,active_message)               
                email_flg = 1
            
            #if last_backup.day != today.day:
            #    last_backup = runBackup()


        # timetosend, minute_of_last_alert = timeToSendAlertMessage(minute_of_last_alert)
        # if SMSAlertEnable and timetosend:
            # print "\n\n**********************\nEvaluating CSV Alert File..."
            # # open csv input file for alerts
            # filename = ("D:\\Dropbox\\Senslope Data\\Proc2\\Monitoring\\alerts\\alertSMS.txt")
            # csvAlertInputFile = open(filename,'r')
            # smsAlertMessage = csvAlertInputFile.read()
            # csvAlertInputFile.close()

##            try:
##                matches = re.findall(regEx,csvAlertText)
##                if not matches:
##                    print "No alerts detected... "
##                else:
##                    for line in matches:
##                        smsAlertMessage = smsAlertMessage.join(line)
##                        csvAlerts += 1
##                    print "Number of alerts: ",csvAlerts
##
##                if len(smsAlertMessage) > 160:
##                    print "Alert message too long. There might be an error."
##                    smsAlertMessage = smsAlertMessage[:10]
##                    
##            except:
##                print "Unknown error detected in reading alert file: ", sys.exc_info()[0]

            # if len(smsAlertMessage)>0:
                # if len(smsAlertMessage)>160:
                    # sms = [smsAlertMessage[:160],smsAlertMessage[160:]]
                # else:
                    # sms = [smsAlertMessage[:160]]

                # for msg in sms:
                    # print "Sending alert message..."

                    # # get report numbers
                    # cfg.read('senslope-server-config.txt')
                    # if network == "SUN":
                        # Numbers = cfg.get('SMSAlert','SmartNumbers')
                    # elif network == "GLOBE":
                        # Numbers = cfg.get('SMSAlert', 'GlobeNumbers') 
                    # else:
                        # Numbers = cfg.get('SMSAlert', 'SmartNumbers')

                    # if Numbers:
                        # for n in Numbers.split(","):            
                            # sendMsg(msg, n)
                        # print "done"
            # print "Alert messaging done...\n\n"
            
            #if last_backup.day != today.day:
            #    last_backup = runBackup()
                
                
    if not FileInput:
        gsm.close()
    test = raw_input('>> End of Code: Press any key to exit')

""" Global variables"""
checkIfActive = True

cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

FileInput = cfg.getboolean('I/O','fileinput')
InputFile = cfg.get('I/O','inputfile')
ConsoleOutput = cfg.getboolean('I/O','consoleoutput')
DeleteAfterRead = cfg.getboolean('I/O','deleteafterread')
SaveToFile = cfg.getboolean('I/O','savetofile')
WriteToDB = cfg.getboolean('I/O','writetodb')

gsm = serial.Serial() 
Baudrate = cfg.getint('Serial', 'Baudrate')
Timeout = cfg.getint('Serial', 'Timeout')
Namedb = cfg.get('LocalDB', 'DBName')
Hostdb = cfg.get('LocalDB', 'Host')
Userdb = cfg.get('LocalDB', 'Username')
Passdb = cfg.get('LocalDB', 'Password')
SleepPeriod = cfg.getint('Misc','SleepPeriod')

# SMS Alerts for columns
##    Numbers = cfg.get('SMSAlert','Numbers')

SMSAlertEnable = cfg.getboolean('SMSAlert','Enable')
Directory = cfg.get('SMSAlert','Directory')
CSVInputFile = cfg.get('SMSAlert','CSVInputFile')
AlertFlags = cfg.get('SMSAlert','AlertFlags')
AlertReportInterval = cfg.getint('SMSAlert','AlertReportInterval')
