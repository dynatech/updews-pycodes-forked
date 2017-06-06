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
		self.io.surficial_plots_path = cfg.get("I/O","surficial_plots_path")
		self.io.surficial_trending_plots_path = cfg.get("I/O","surficial_trending_plots_path")
		self.io.surficial_meas_plots_path = cfg.get("I/O","surficial_meas_plots_path")
          
		self.io.CSVFormat = cfg.get('I/O','CSVFormat')
		self.io.PrintPlot = cfg.getboolean('I/O','PrintPlot')
		self.io.PrintSummaryAlert = cfg.getboolean('I/O','PrintSummaryAlert')
		self.io.PrintTrendPlot = cfg.getboolean('I/O','PrintTrendPlot')
		self.io.PrintMeasPlot = cfg.getboolean('I/O','PrintMeasPlot')


		self.io.data_dt = cfg.getfloat("I/O","data_dt")
		self.io.rt_window_length = cfg.getfloat("I/O","rt_window_length")
		self.io.roll_window_length = cfg.getfloat("I/O","roll_window_length")
		self.io.surficial_num_pts = cfg.getfloat("I/O","surficial_num_pts")
		self.io.meas_plot_window = cfg.getfloat("I/O","meas_plot_window")
		
		self.values = Container()
		self.values.slope = cfg.getfloat("CI Values","slope")
		self.values.intercept = cfg.getfloat("CI Values","intercept")
		self.values.t_crit = cfg.getfloat("CI Values","t_crit")
		self.values.var_v_log = cfg.getfloat("CI Values","var_v_log")
		self.values.v_log_mean = cfg.getfloat("CI Values","v_log_mean")
		self.values.sum_res_square = cfg.getfloat("CI Values","sum_res_square")
		self.values.n = cfg.getfloat("CI Values","n")

		self.thresh = Container()
		self.thresh.v_alert_2 = cfg.getfloat("Thresh Values","v_alert_2")
		self.thresh.v_alert_3 = cfg.getfloat("Thresh Values","v_alert_3")

