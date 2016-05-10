import serial, datetime, ConfigParser, time, re
from datetime import datetime as dt
from senslopedbio import * 

cfg = ConfigParser.ConfigParser()
cfg.read(sys.path[0] + '/' + 'senslope-server-config.txt')

gsm = serial.Serial()
Baudrate = cfg.getint('Serial', 'Baudrate')
Timeout = cfg.getint('Serial', 'Timeout')
Namedb = cfg.get('LocalDB', 'DBName')
SaveToFile = cfg.get('I/O', 'savetofile')

class sms:
    def __init__(self,num,sender,data,dt):
       self.num = num
       self.simnum = sender
       self.data = data
       self.dt = dt
       
def gsmInit(network):
    Port = cfg.get('Serial', network+'port')
    print 'Connecting to GSM modem at', Port
    
    if Port.find("COM")>0:
        gsm.port = int(re.search("\d+").group(0)) - 1
    else:
        gsm.port = Port
    gsm.baudrate = Baudrate
    gsm.timeout = Timeout
    gsm.open()
    #gsmflush()
    print 'Switching to no-echo mode', gsmcmd('ATE0').strip('\r\n')
    print 'Switching to text mode', gsmcmd('AT+CMGF=1').rstrip('\r\n')
    
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
        # RunSenslopeServer(gsm_network)
        
def sendMsg(msg, number):
    """
    Sends a command 'cmd' to GSM Module
    Returns the reply of the module
    Usage: str = gsmcmd()
    """
    # under development
    # return
    try: 
        a = ''
        now = time.time()
        preamble = "AT+CMGS=\""+number+"\""
        print "\nMSG:", msg
        print "NUM:", number
        gsm.write(preamble+"\r")
        now = time.time()
        while a.find('>')<0 and a.find("ERROR")<0 and time.time()<now+20:
            a += gsm.read(gsm.inWaiting())
            time.sleep(0.5)
            print '.',

        if time.time()>now+3 or a.find("ERROR") > -1:  
            print '>> Error: GSM Unresponsive at finding >'
            print a
            print '^^ a ^^'
            return -1
        else:
            print '>'
        
        a = ''
        now = time.time()
        gsm.write(msg+chr(26))
        while a.find('OK')<0 and a.find("ERROR")<0 and time.time()<now+60:
                a += gsm.read(gsm.inWaiting())
                time.sleep(0.5)
                print ':',
        if time.time()-60>now:
            print '>> Error: timeout reached'
            return -1
        elif a.find('ERROR')>-1:
            print '>> Error: GSM reported ERROR in SMS reading'
            return -1
        else:
            print ">> Message sent!"
            return 0
            
    except serial.SerialException:
        print "NO SERIAL COMMUNICATION (sendmsg)"
        RunSenslopeServer(gsm_network)	
        
def logError(log):
    nowdate = dt.today().strftime("%A, %B %d, %Y, %X")
    f = open("errorLog.txt","a")
    f.write(nowdate+','+log.replace('\r','%').replace('\n','%') + '\n')
    f.close()
    

def countmsg():
    """
    Gets the # of SMS messages stored in GSM modem.
    Usage: c = countmsg()
    """
    while True:
        b = ''
        c = ''
        b = gsmcmd('AT+CPMS?')
        
        try:
            c = int( b.split(',')[1] )
            #print '>>>> ', c
            return c
        except IndexError:
            print 'count_msg b = ',b
            logError(b)
            if b:
                return 0                
            else:
                return -1
                
            ##if GSM sent blank data maybe GSM is inactive
        except ValueError:
            print '>> ValueError:'
            print b
            print '>> Retryring message reading'
            logError(b)
            # return -2   

def getAllSms(network):
    allmsgs = 'd' + gsmcmd('AT+CMGL="ALL"')
    allmsgs = allmsgs.replace("\r\nOK\r\n",'').split("+CMGL")[1:]
    if allmsgs:
        temp = allmsgs.pop(0) #removes "=ALL"
        
    msglist = []
    
    for msg in allmsgs:
        # if SaveToFile:
            # mon = dt.now().strftime("-%Y-%B-")
            # f = open("D:\\Server Files\\Consolidated\\"+network+mon+'backup.txt','a')
            # f.write(msg)
            # f.close()
                
        msg = msg.replace('\n','').split("\r")
        try:
            txtnum = re.search(r': [0-9]{1,2},',msg[0]).group(0).strip(': ,')
        except AttributeError:
            # particular msg may be some extra strip of string 
            print ">> Error: message may not have correct construction", msg[0]
            logError("wrong construction\n"+msg[0])
            continue
        
        try:
            sender = re.search(r'[0-9]{11,12}',msg[0]).group(0)
        except AttributeError:
            print 'Sender unknown.', msg[0]
            sender = "UNK"
            
        try:
            txtdatetimeStr = re.search(r'\d\d/\d\d/\d\d,\d\d:\d\d:\d\d',msg[0]).group(0)
            txtdatetime = dt.strptime(txtdatetimeStr,'%y/%m/%d,%H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
        except:
            print "Error in date time conversion"
                
        smsItem = sms(txtnum, sender, msg[1], txtdatetimeStr)
        
        msglist.append(smsItem)
        
    
    return msglist
        