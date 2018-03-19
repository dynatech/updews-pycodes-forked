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


cfgfiletxt = 'server_config.txt'
cfile = os.path.dirname(os.path.realpath(__file__)) + '/' + cfgfiletxt
    
def read_cfg_file():
    cfg = ConfigParser.ConfigParser()
    cfg.read(cfile)
    return cfg

def save_cfg_changes(cfg):
    with open(cfile, 'wb') as c:
        cfg.write(c)

class Container(object):
	pass

class dewsl_server_config:
	def __init__(self):
		self.version = 1

		cfg = read_cfg_file()

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

		cfg = read_cfg_file()            
		self.cfg = cfg

		self.dbhost = dict()
		for opt in cfg.options("Hosts"):
			self.dbhost[opt] = cfg.get("Hosts",opt)

		self.db = dict()
		for opt in cfg.options("Db"):
			self.db[opt] = cfg.get("Db",opt)

def main():
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	
	# new server config
	c = dewsl_server_config()
	mc.set("server_config",c.config)
	# print c.config['gsmdb']['username']

	# old server config
	sc = config()
	mc.set('sc',sc)

	return

if __name__ == "__main__":
    main()