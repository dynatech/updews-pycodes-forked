import ConfigParser, os

# USAGE
# 
# import cfgfileio as cfg
# 
# s = cfg.config()
# print s.io.roll_window_length


cfgfiletxt = 'IO-Config.txt'
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

		self.io = Container()
		self.io.output_file_path = cfg.get("I/O","OutputFilePath")
		self.io.RainfallPlotsPath = cfg.get("I/O","RainfallPlotsPath")
          
		self.io.CSVFormat = cfg.get('I/O','CSVFormat')
		self.io.PrintPlot = cfg.getboolean('I/O','PrintPlot')
		self.io.PrintSummaryAlert = cfg.getboolean('I/O','PrintSummaryAlert')

		self.io.data_dt = cfg.getfloat("I/O","data_dt")
		self.io.rt_window_length = cfg.getfloat("I/O","rt_window_length")
		self.io.roll_window_length = cfg.getfloat("I/O","roll_window_length")