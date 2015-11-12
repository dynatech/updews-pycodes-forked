import serial, datetime, ConfigParser, time

cfg = ConfigParser.ConfigParser()
cfg.read('senslope-server-config.txt')

gsm = serial.Serial()
Baudrate = cfg.getint('Serial', 'Baudrate')
Timeout = cfg.getint('Serial', 'Timeout')
ConsoleOutput = cfg.getboolean('I/O','consoleoutput')

class sms:
    def __init__(self,num,data,sender,dt):
       self.num = ''
       self.sender = ''
       self.data = ''
       self.dt = ''
       
def gsmInit(port):
    print 'Connecting to GSM modem at COM',
    gsm.port = port
    gsm.baudrate = Baudrate
    gsm.timeout = Timeout
    gsm.open()
    #gsmflush()
    print port+1
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
        # RunSenslopeServer(gsm_network)
        
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
        
