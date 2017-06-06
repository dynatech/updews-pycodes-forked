import ConfigParser, os

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


cfgfiletxt = 'serverconfig.txt'
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

		self.dbio = Container()
		self.dbio.hostdb = cfg.get("DB I/O","hostdb")
		self.dbio.userdb = cfg.get("DB I/O","userdb")
		self.dbio.passdb = cfg.get("DB I/O","passdb")
		self.dbio.namedb = cfg.get("DB I/O","namedb")
		self.dbio.printtostdout = cfg.get("DB I/O","printtostdout")

		self.value = Container()
		self.value.limitvalues = cfg.getboolean("Value Limits","limitvalues")
		self.value.xlim = cfg.getint("Value Limits","xlim")
		self.value.ylim = cfg.getint("Value Limits","ylim")
		self.value.zlim = cfg.getint("Value Limits","zlim")
		self.value.xmax = cfg.getint("Value Limits","xmax")
		self.value.mlowlim = cfg.getint("Value Limits","mlowlim")
		self.value.muplim = cfg.getint("Value Limits","muplim")
		self.value.cutoff = cfg.getfloat("Value Limits","cutoff")
		self.value.moniterval = cfg.getint("Value Limits","moninterval")

		self.filtargs = Container()
		self.filtargs.window = cfg.getint("Filter Args","window")
		self.filtargs.order = cfg.getint("Filter Args","order")
		self.filtargs.off_lim = cfg.getint("Filter Args","off_lim")

		self.dtrange = Container()
		self.dtrange.rangedef = cfg.getint("Datetime range","rangedefault")

		self.misc = Container()
		self.misc.debug = cfg.getboolean("Misc","debug")
		self.misc.dotproductthreshold = cfg.getfloat("Misc","dotproductthreshold")

		self.io = Container()
		self.io.outputfilepath = cfg.get("I/O","outputfilepath")
		self.io.rainfallplotspath = cfg.get("I/O","rainfallplotspath")
		self.io.grndmeasplotspath = cfg.get("I/O","grndmeasplotspath")
		self.io.eq_plots_path = cfg.get("I/O","eq_plots_path")
          
		self.io.t_disp = cfg.getfloat("I/O","t_disp")
		self.io.t_vell2 = cfg.getfloat("I/O","t_vell2")
		self.io.t_vell3 = cfg.getfloat("I/O","t_vell3")
		self.io.k_ac_ax = cfg.getfloat("I/O","k_ac_ax")
		self.io.num_nodes_to_check = cfg.getint("I/O","num_nodes_to_check")

		self.io.data_dt = cfg.getfloat("I/O","data_dt")
		self.io.rt_window_length = cfg.getfloat("I/O","rt_window_length")
		self.io.roll_window_length = cfg.getfloat("I/O","roll_window_length")
		self.io.num_roll_window_ops = cfg.getint("I/O","num_roll_window_ops")
		self.io.col_pos_interval = str(cfg.get("I/O","col_pos_interval"))
		self.io.num_col_pos = cfg.getint("I/O","num_col_pos")
		self.io.to_fill = cfg.getint("I/O","to_fill")
		self.io.to_smooth = cfg.getint("I/O","to_smooth")
		self.io.column_fix = cfg.get("I/O","column_fix")
  
		self.io.rt_to_fill = cfg.getint("I/O","rt_to_fill")
		self.io.rt_to_smooth = cfg.getint("I/O","rt_to_smooth")

		self.rainfall = Container()
		self.rainfall.PrintPlot = cfg.getboolean('rainfall','PrintPlot')
		self.rainfall.PrintSummaryAlert = cfg.getboolean('rainfall','PrintSummaryAlert')

		self.rainfall.data_dt = cfg.getfloat("rainfall","data_dt")
		self.rainfall.rt_window_length = cfg.getfloat("rainfall","rt_window_length")
		self.rainfall.roll_window_length = cfg.getfloat("rainfall","roll_window_length")