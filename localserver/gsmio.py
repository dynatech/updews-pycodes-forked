import serial, datetime, ConfigParser, time, re
from datetime import datetime as dt
from datetime import timedelta as td
import serverdbio as dbio 
from messaging.sms import SmsDeliver as smsdeliver
from messaging.sms import SmsSubmit as smssubmit
import cfgfileio as cfg
import argparse
from random import random

if cfg.config().mode.script_mode == 'gsmserver':
    import RPi.GPIO as GPIO

    resetpin = cfg.config().gsmio.resetpin
    gsm = ''

    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(resetpin, GPIO.OUT)

class sms:
    def __init__(self,num,sender,data,dt):
       self.num = num
       self.simnum = sender
       self.data = data
       self.dt = dt

class CustomGSMResetException(Exception):
    pass

def power_gsm(mode):
    GPIO.output(cfg.config().gsmio.resetpin, mode)
    print 'done'
    
def reset_gsm():
    print ">> Resetting GSM Module ...",
    try:
        power_gsm(False)
        time.sleep(2)
        power_gsm(True)
        print 'done'
    except ImportError:
        return
       
def init_gsm(network):
    global gsm
    power_gsm(True)
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
    
    #flush_gsm()
    for i in range(0,4):
        gsm.write('AT\r\n')
        time.sleep(1)
    print 'Switching to no-echo mode', gsm_cmd('ATE0').strip('\r\n')
    print 'Switching to PDU mode', gsm_cmd('AT+CMGF=0').rstrip('\r\n')
    print 'Disabling unsolicited CMTI', gsm_cmd('AT+CNMI=2,0,0,0,0').rstrip('\r\n')

    return gsm
    
def flush_gsm():
    """Removes any pending inputs from the GSM modem and checks if it is alive."""
    try:
        gsm.flushInput()
        gsm.flushOutput()
        ghost = gsm.read(gsm.inWaiting())
        stat = gsm_cmd('\x1a\rAT\r')    
        while('E' in stat):
            gsm.flushInput()
            gsm.flushOutput()
            ghost = gsm.read(gsm.inWaiting())
            stat = gsm_cmd('\x1a\rAT\r')
    except serial.SerialException:
        print "NO SERIAL COMMUNICATION (flush_gsm)"
        sys.exit()
        # RunSenslopeServer(gsm_network)

def gsm_cmd(cmd):
    """
    Sends a command 'cmd' to GSM Module
    Returns the reply of the module
    Usage: str = gsm_cmd()
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
            except_str = (">> Raising exception to reset code "
                "from GSM module reset")
            raise CustomGSMResetException()
        return a
    except serial.SerialException:
        print "NO SERIAL COMMUNICATION (gsm_cmd)"
        # RunSenslopeServer(gsm_network)

def send_msg(msg, number, simulate=False):
    """
    Sends a command 'cmd' to GSM Module
    Returns the reply of the module
    Usage: str = gsm_cmd()
    """
    # under development
    # return
    # simulate sending success
    # False => sucess
    if simulate:
        return random() < 0.15

    try:
        pdulist = smssubmit(number,msg).to_pdu()
    except:
        print "Error in pdu conversion. Skipping message sending"
        return -1

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
            print "NO SERIAL COMMUNICATION (send_msg)"
            RunSenslopeServer(gsm_network)  

    return 0
        
def log_error(log):
    nowdate = dt.today().strftime("%A, %B %d, %Y, %X")
    f = open("errorLog.txt","a")
    f.write(nowdate+','+log.replace('\r','%').replace('\n','%') + '\n')
    f.close()
    

def count_msg():
    """
    Gets the # of SMS messages stored in GSM modem.
    Usage: c = count_msg()
    """
    while True:
        b = ''
        c = ''
        b = gsm_cmd('AT+CPMS?')
        
        try:
            c = int( b.split(',')[1] )
            print '\n>> Received', c, 'message/s'
            return c
        except IndexError:
            print 'count_msg b = ',b
            # log_error(b)
            if b:
                return 0                
            else:
                return -1
                
            ##if GSM sent blank data maybe GSM is inactive
        except ValueError:
            print '>> ValueError:'
            print b
            print '>> Retryring message reading'
            # log_error(b)
            # return -2   

def get_all_sms(network):
    allmsgs = 'd' + gsm_cmd('AT+CMGL=4')
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
            # log_error("wrong construction\n"+msg[0])
            continue

        # print pdu

        smsdata = smsdeliver(pdu).data

        try:
            txtnum = re.search(r'(?<= )[0-9]{1,2}(?=,)',msg).group(0)
        except AttributeError:
            # particular msg may be some extra strip of string 
            print ">> Error: message may not have correct construction", msg
            # log_error("wrong construction\n"+msg[0])
            continue
        
        txtdatetimeStr = smsdata['date'] + td(hours=8)

        txtdatetimeStr = txtdatetimeStr.strftime('%Y-%m-%d %H:%M:%S')

#        print smsdata['text']
        try:        
            smsItem = sms(txtnum, smsdata['number'].strip('+'), 
                str(smsdata['text']), txtdatetimeStr)
            print str(smsdata['text'])
            msglist.append(smsItem)
        except UnicodeEncodeError:
            print ">> Unknown character error. Skipping message"
            continue

    return msglist


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="RPI GSM command options")
    parser.add_argument("-r", "--reset_gsm", help="hard reset of gsm modules", 
        action="store_true")
    try:
        args = parser.parse_args()
    except:
        print "Error in parsing"

    if args.reset_gsm:
        print "> Resetting GSM module"
        reset_gsm()
        
