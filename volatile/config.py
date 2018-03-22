import memory
from datetime import datetime as dt
import os
import ConfigParser

def read_cfg_file():
	cfgfiletxt = 'dyna_config.cfg'
	cfile = os.path.dirname(os.path.realpath(__file__)) + '/' + cfgfiletxt
	cfg = ConfigParser.ConfigParser()
	cfg.read(cfile)
	return cfg

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

def main():

	print dt.today().strftime('%Y-%m-%d %H:%M:%S')	
	mc = memory.get_handle()
	
	print 'Setting server configuration',
	c = dewsl_server_config()
	print c.config
	mc.set("server_config",c.config)
	print "... done"

if __name__ == "__main__":
    main()