import ConfigParser, os, serial
import memcache

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

class dewslserverconfig:
	def __init__(self):
		self.version = 1

		cfg = readCfgFile()

		self.config = dict()  

		for section in cfg.sections():
			options = dict()
			for opt in cfg.options(section):

				try:
					options[opt] = cfg.getboolean(section, opt)
					continue
				except ValueError:
					# may not be booelan
					pass

				try:
					options[opt] = cfg.getint(section, opt)
					continue
				except ValueError:
					# may not be integer
					pass

				# should be a string
				options[opt] = cfg.get(section, opt)

			# setattr(self, section.lower(), options)
			self.config[section.lower()] = options

class config:
	def __init__(self):

		cfg = readCfgFile()            
		self.cfg = cfg

		self.dbhost = dict()
		for opt in cfg.options("Hosts"):
			self.dbhost[opt] = cfg.get("Hosts",opt)

		self.db = dict()
		for opt in cfg.options("Db"):
			self.db[opt] = cfg.get("Db",opt)

		self.serialio = Container()
		self.serialio.baudrate = cfg.getint("Serial","baudrate")
		self.serialio.globeport = cfg.get("Serial","globeport")
		self.serialio.smartport = cfg.get("Serial","smartport")
		self.serialio.timeout = cfg.getint("Serial","timeout")

		self.gsmio = Container()
		self.gsmio.resetpin = cfg.getint("gsmio","resetpin")
		self.gsmio.sim_gsm = cfg.getboolean("gsmio","simulate_gsm")		
		
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
		self.fileio.eqprocfile = cfg.get("FileIO","eqprocfile")
		self.fileio.queryoutput = cfg.get("FileIO","querylatestreportoutput")
		self.fileio.alertgenscript = cfg.get("FileIO","alertgenscript")
		self.fileio.alertanalysisscript = cfg.get("FileIO","alertanalysisscript")
		self.fileio.websocketdir = cfg.get("FileIO","websocketdir")
		self.fileio.gndalert1 = cfg.get("FileIO","gndalert1")
		self.fileio.gndalert2 = cfg.get("FileIO","gndalert2")
		self.fileio.gndalert3 = cfg.get("FileIO","gndalert3")		
		
		self.simprefix = Container()
		self.simprefix.smart = cfg.get("simprefix","smart")
		self.simprefix.globe = cfg.get("simprefix","globe")

		self.mode = Container()
		self.mode.script_mode = cfg.get("mode","script_mode")
		if self.mode.script_mode == 'gsmserver':
			self.mode.sendmsg = True
			self.mode.procmsg = False
			self.mode.logtoinstance = 'GSM'
		elif self.mode.script_mode == 'procmsg':
			self.mode.sendmsg = False
			self.mode.procmsg = True
			self.mode.logtoinstance = 'LOCAL'

		self.io = Container()
		self.io.proc_limit = cfg.getint("io","proc_limit")
		self.io.active_lgr_limit = cfg.getint("io","active_lgr_limit")
		

def main():
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	
	# new server config
	c = dewslserverconfig()
	mc.set("server_config",c.config)
	# print c.config['gsmdb']['username']

	# old server config
	sc = config()
	mc.set('sc',sc)

	return

if __name__ == "__main__":
    main()