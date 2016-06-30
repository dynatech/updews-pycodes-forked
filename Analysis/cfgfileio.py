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


cfgfiletxt = 'server-config.txt'
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
		self.io.ndfilepath = cfg.get("I/O","ndfilepath")
		self.io.outputfilepath = cfg.get("I/O","outputfilepath")
		self.io.procfilepath = cfg.get("I/O","procfilepath")
		self.io.colalertsfilepath = cfg.get("I/O","colalertsfilepath")
		self.io.trendalertsfilepath = cfg.get("I/O","trendalertsfilepath")
		self.io.alertanalysispath = cfg.get("I/O","alertanalysispath")
		self.io.rainfallplotspath = cfg.get("I/O","rainfallplotspath")

		self.io.t_disp = cfg.getfloat("I/O","t_disp")
		self.io.t_vell2 = cfg.getfloat("I/O","t_vell2")
		self.io.t_vell3 = cfg.getfloat("I/O","t_vell3")
		self.io.k_ac_ax = cfg.getfloat("I/O","k_ac_ax")
		self.io.num_nodes_to_check = cfg.getint("I/O","num_nodes_to_check")

		self.io.col_props_headers = cfg.get("I/O","columnproperties_headers")
		self.io.purged_file_headers = cfg.get("I/O","purged_file_headers")
		self.io.mon_file_headers = cfg.get("I/O","monitoring_file_headers")
		self.io.lgd_file_headers = cfg.get("I/O","lastgooddata_file_headers")
		self.io.proc_mon_file_headers = cfg.get("I/O","proc_monitoring_file_headers")
		self.io.alerteval_colarrange = cfg.get("I/O","alerteval_colarrange")
		self.io.alertgen_headers = cfg.get("I/O","alertgen_headers")
		self.io.alert_headers = cfg.get("I/O","alert_headers")

		self.io.data_dt = cfg.getfloat("I/O","data_dt")
		self.io.rt_window_length = cfg.getfloat("I/O","rt_window_length")
		self.io.roll_window_length = cfg.getfloat("I/O","roll_window_length")
		self.io.num_roll_window_ops = cfg.getint("I/O","num_roll_window_ops")
		self.io.col_pos_interval = cfg.get("I/O","col_pos_interval")
		self.io.num_col_pos = cfg.getint("I/O","num_col_pos")
		self.io.alert_time_int = cfg.getint("I/O","alert_time_int")
		self.io.to_fill = cfg.getint("I/O","to_fill")
		self.io.to_smooth = cfg.getint("I/O","to_smooth")
		
		self.io.with_trendingnodealert = cfg.getboolean("I/O","with_trendingnodealert")
		self.io.test_sites = cfg.get("I/O","test_sites")
		self.io.test_specific_sites = cfg.get("I/O","test_specific_sites")

		self.io.test_specific_time = cfg.getboolean("I/O","test_specific_time")
		self.io.use_specific_time = cfg.get("I/O","use_specific_time")
		self.io.realtime_specific_sites = cfg.get("I/O","realtime_specific_sites")
		self.io.rt_to_fill = cfg.getint("I/O","rt_to_fill")
		self.io.rt_to_smooth = cfg.getint("I/O","rt_to_smooth")

		self.io.plotcolpos = cfg.getboolean("I/O","plotcolpos")
		self.io.plotdispvel = cfg.getboolean("I/O","plotdispvel")
		self.io.printproc = cfg.getboolean("I/O","printproc")
		self.io.printtrendalerts = cfg.getboolean("I/O","printtrendalerts")
		self.io.printwalert = cfg.getboolean("I/O","printwalert")
		self.io.printnd = cfg.getboolean("I/O","printnd")
		self.io.printtimer = cfg.getboolean("I/O","printtimer")
		self.io.printtalert = cfg.getboolean("I/O","printtalert")
		self.io.printtalert2 = cfg.getboolean("I/O","printtalert2")
		self.io.printaalert = cfg.getboolean("I/O","printaalert")
		self.io.printgsmalert = cfg.getboolean("I/O","printgsmalert")
		self.io.printjson = cfg.getboolean("I/O","printjson")
		self.io.printgalert = cfg.getboolean("I/O","printgalert")
		

		


		









		


		
