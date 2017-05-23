import memcache
import cfgfileio as cfg
import sys
import subprocess
import lockscript, time
from datetime import datetime as dt


def countAlertAnalysisInstances():
	p = subprocess.Popen("ps ax | grep alertgen.py -c", stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
	out, err = p.communicate()
	return int(out)

def main():

	lockscript.get_lock('alertgenexec')

	print dt.today().strftime("%c")

	c = cfg.config()

	ongoing = []
	
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)

	proc_limit = c.io.proc_limit
	
	while True:
		alertgenlist = mc.get('alertgenlist')

		print alertgenlist

		if alertgenlist is None:
			break

		if len(alertgenlist) == 0:
			break

		alert_info = alertgenlist.pop()

		mc.set('alertgenlist',[])
		mc.set('alertgenlist',alertgenlist)

		command = "python %s %s '%s'" % (c.fileio.alertgenscript,alert_info['tsm_name'],alert_info['ts'])

		print "Running", alert_info['tsm_name'], "alertgen"
        
		if lockscript.get_lock('alertgen for %s' % alert_info['tsm_name'],exitifexist=False):
			p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
		else:
			continue

		while countAlertAnalysisInstances() > proc_limit:
			time.sleep(5)
			print '.',	


if __name__ == "__main__":
    main()