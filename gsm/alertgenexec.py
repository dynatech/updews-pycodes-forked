from datetime import datetime as dt
import os
import subprocess
import sys
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import lockscript
import volatile.memory as mem


def count_alert_analysis_instances():
	p = subprocess.Popen("ps ax | grep alertgen.py -c", stdout=subprocess.PIPE, 
		shell=True, stderr=subprocess.STDOUT)
	out, err = p.communicate()
	return int(out)

def main(mc):
	print (dt.today().strftime("%c"))

	sc = mem.server_config()

	proc_limit = sc["io"]["proc_limit"]
	
	while True:
		alertgenlist = mc.get('alertgenlist')

		print (alertgenlist)

		if alertgenlist is None:
			break

		if len(alertgenlist) == 0:
			break

		alert_info = alertgenlist.pop()

		mc.set('alertgenlist',[])
		mc.set('alertgenlist',alertgenlist)

		python_path = sc['fileio']['python_path']
		alertgen = sc["fileio"]["alertgenscript"]
		command = '{} {} {} "{}"'.format(python_path, alertgen, alert_info['tsm_name'], alert_info['ts'])

		print ("Running", alert_info['tsm_name'], "alertgen")
        
		if lockscript.get_lock('alertgen for %s' % alert_info['tsm_name'], 
			exitifexist=False):
			print('execute:', command)
			run_out = subprocess.run(command, stdout=subprocess.PIPE, shell=True, 
				stderr=subprocess.STDOUT)
			print(run_out)
		else:
			continue

		while count_alert_analysis_instances() > proc_limit:
			time.sleep(5)
			print ('.',)



if __name__ == "__main__":

	mc = mem.get_handle()
	ag_instance = mc.get('alertgenexec')

	if ag_instance == True:
		print ('Process already exists (via memcache).. aborting')
		sys.exit()

	mc.set('alertgenexec', True)
	try:
		main(mc)
	except KeyboardInterrupt:
		print ('Unexpected Error')
	mc.set('alertgenexec', False)
