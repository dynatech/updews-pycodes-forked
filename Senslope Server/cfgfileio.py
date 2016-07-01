import ConfigParser, os, serial

# USAGE
# 
# 
# import cfgfileio as cfg
# 
# s = cfg.config()
# print s.dbio.hostdb
# print s.io.rt_to_fill
# print s.io.printtimer
# print s.misc.debug


cfgfiletxt = 'senslope-server-config.txt'
cfile = os.path.dirname(os.path.realpath(__file__)) + '/' + cfgfiletxt
    
def readCfgFile():
    cfg = ConfigParser.ConfigParser()
    cfg.read(cfile)
    return cfg

def saveConfigChanges(cfg):
    with open(cfile, 'wb') as c:
        cfg.write(c)

class Container(object):
	pass
        
class config:
	def __init__(self):

		cfg = readCfgFile()            
		self.cfg = cfg

		self.localdb = Container()
		self.localdb.user = cfg.get("LocalDB","username")
		self.localdb.host = cfg.get("LocalDB","host")
		self.localdb.pwd = cfg.get("LocalDB","password")
		self.localdb.name = cfg.get("LocalDB","dbname")
		
		self.gsmdb = Container()
		self.gsmdb.user = cfg.get("GSMDB","username")
		self.gsmdb.host = cfg.get("GSMDB","host")
		self.gsmdb.pwd = cfg.get("GSMDB","password")
		self.gsmdb.name = cfg.get("GSMDB","dbname")

		self.serialio = Container()
		self.serialio.baudrate = cfg.getint("Serial","baudrate")
		self.serialio.globeport = cfg.get("Serial","globeport")
		self.serialio.smartport = cfg.get("Serial","smartport")
		self.serialio.timeout = cfg.getint("Serial","timeout")
		
		self.smsalert = Container()
		self.smsalert.communitynum = cfg.get("SMSAlert","communityphonenumber")
		self.smsalert.sunnum = cfg.get("SMSAlert","sunnumbers")
		self.smsalert.globenum = cfg.get("SMSAlert","globenumbers")
		self.smsalert.smartnum = cfg.get("SMSAlert","smartnumbers")
		self.smsalert.serveralert = cfg.get("SMSAlert","serveralert")

		self.reply = Container()
		self.reply.successen = cfg.get("ReplyMessages","successen")
		self.reply.successtag = cfg.get("ReplyMessages","successtag")
		self.reply.faildateen = cfg.get("ReplyMessages","faildateen")
		self.reply.failtimeen = cfg.get("ReplyMessages","failtimeen")
		self.reply.failmeasen = cfg.get("ReplyMessages","failmeasen")
		self.reply.failweaen = cfg.get("ReplyMessages","failweaen")
		self.reply.failobven = cfg.get("ReplyMessages","failobven")
		
		self.fileio = Container()
		self.fileio.allalertsfile = cfg.get("FileIO","allalertsfile")
		self.fileio.queryoutput = cfg.get("FileIO","querylatestreportoutput")
		
		self.simprefix = Container()
		self.simprefix.smart = cfg.get("simprefix","smart")
		self.simprefix.globe = cfg.get("simprefix","globe")

