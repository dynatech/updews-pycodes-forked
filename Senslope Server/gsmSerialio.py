import serial, datetime, ConfigParser, time, re
from datetime import datetime as dt
from datetime import timedelta as td
import senslopedbio as dbio 
from messaging.sms import SmsDeliver as smsdeliver
from messaging.sms import SmsSubmit as smssubmit
import cfgfileio as cfg

gsm = ''

class sms:
    def __init__(self,num,sender,data,dt):
       self.num = num
       self.simnum = sender
       self.data = data
       self.dt = dt

class CustomGSMResetException(Exception):
    pass

def resetGsm():
    print ">> Resetting GSM Module ...",
    resetPin = 38
    try:
        import RPi.GPIO as GPIO
        GPIO.setmode(GPIO.BOARD)
        GPIO.setup(resetPin, GPIO.OUT)

        GPIO.output(resetPin, True)
        time.sleep(1)
        GPIO.output(resetPin, False)
        time.sleep(5)
        GPIO.output(resetPin, True)
        gsm.close()
        print 'done'
        raise CustomGSMResetException(">> Raising exception to reset code from GSM module reset")
    except ImportError:
        return
       
def gsmInit(network):
    global gsm
    gsm = serial.Serial()
    c = cfg.config()
    if network.lower() == 'globe':
        Port = c.serialio.globeport
    else:
        Port = c.serialio.smartport
    print 'Connecting to GSM modem at', Port
    
    gsm.port = Port
    gsm.baudrate = c.serialio.baudrate
    gsm.timeout = c.serialio.timeout
    
    if(gsm.isOpen() == False):
        gsm.open()
    
    #gsmflush()
    gsm.write('AT\r\n')
    time.sleep(1)
    gsm.write('AT\r\n')
    time.sleep(1)
    gsm.write('AT\r\n')
    time.sleep(1)
    print 'Switching to no-echo mode', gsmcmd('ATE0').strip('\r\n')
    print 'Switching to text mode', gsmcmd('AT+CMGF=0').rstrip('\r\n')

    return gsm
    
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
        sys.exit()
        # RunSenslopeServer(gsm_network)

def gsmcmd(cmd):
    """
    Sends a command 'cmd' to GSM Module
    Returns the reply of the module
    Usage: str = gsmcmd()
    """
    global gsm

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

    pdulist = smssubmit(number,msg).to_pdu()

    # print "pdulen", len(pdulist)

    for pdu in pdulist:
        try: 
            a = ''
            now = time.time()
            preamble = "AT+CMGS=%d" % (pdu.length)

            # print preamble

            print "\nMSG:", msg, 
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
            gsm.write(pdu.pdu+chr(26))
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
                
                
        except serial.SerialException:
            print "NO SERIAL COMMUNICATION (sendmsg)"
            RunSenslopeServer(gsm_network)  

    return 0
        
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
            print '>> Received', c, 'message/s'
            return c
        except IndexError:
            print 'count_msg b = ',b
            # logError(b)
            if b:
                return 0                
            else:
                return -1
                
            ##if GSM sent blank data maybe GSM is inactive
        except ValueError:
            print '>> ValueError:'
            print b
            print '>> Retryring message reading'
            # logError(b)
            # return -2   

def getAllSms(network):
    allmsgs = 'd' + gsmcmd('AT+CMGL=4')
    # print allmsgs.replace('\r','@').replace('\n','$')
    # allmsgs = allmsgs.replace("\r\nOK\r\n",'').split("+CMGL")[1:]

    allmsgs = re.findall("(?<=\+CMGL:).+\r\n.+(?=\n*\r\n\r\n)",allmsgs)
    #if allmsgs:
    #    temp = allmsgs.pop(0) #removes "=ALL"
    msglist = []
    
    for msg in allmsgs:
        # if SaveToFile:
            # mon = dt.now().strftime("-%Y-%B-")
            # f = open("D:\\Server Files\\Consolidated\\"+network+mon+'backup.txt','a')
            # f.write(msg)
            # f.close()
                
        # msg = msg.replace('\n','').split("\r")
        try:
            pdu = re.search(r'[0-9A-F]{20,}',msg).group(0)
        except AttributeError:
            # particular msg may be some extra strip of string 
            print ">> Error: cannot find pdu text", msg
            # logError("wrong construction\n"+msg[0])
            continue

        # print pdu

        smsdata = smsdeliver(pdu).data

        try:
            txtnum = re.search(r'(?<= )[0-9]{1,2}(?=,)',msg).group(0)
        except AttributeError:
            # particular msg may be some extra strip of string 
            print ">> Error: message may not have correct construction", msg
            # logError("wrong construction\n"+msg[0])
            continue
        
        # try:
        #     sender = re.search(r'[0-9]{11,12}',msg[0]).group(0)
        # except AttributeError:
        #     print 'Sender unknown.', msg[0]
        #     sender = "UNK"
            
        # try:
        #     txtdatetimeStr = re.search(r'\d\d/\d\d/\d\d,\d\d:\d\d:\d\d',msg[0]).group(0)
        #     txtdatetime = dt.strptime(txtdatetimeStr,'%y/%m/%d,%H:%M:%S').strftime('%Y-%m-%d %H:%M:00')
        # except:
        #     print "Error in date time conversion"
        txtdatetimeStr = smsdata['date'] + td(hours=8)

        txtdatetimeStr = txtdatetimeStr.strftime('%Y-%m-%d %H:%M:%S')

#        print smsdata['text']
        try:        
            smsItem = sms(txtnum, smsdata['number'].strip('+'), str(smsdata['text']), txtdatetimeStr)
            msglist.append(smsItem)
        except UnicodeEncodeError:
            print ">> Unknown character error. Skipping message"
            continue
        # print str(smsdata['text'])
        
#        msglist.append(smsItem)
        
    
    return msglist
        
