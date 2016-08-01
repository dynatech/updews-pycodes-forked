import memcache
import cfgfileio as cfg
import sys
import subprocess
import lockscript
from datetime import datetime as dt

def main():

	lockscript.get_lock('alertgenexec')

	print dt.today().strftime("%c")

	c = cfg.config()

	ongoing = []
	
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)

	# mc.set('alertgenlist',[])
	# sys.exit()

	proc_limit = 3
	
	while True:
		alertgenlist = mc.get('alertgenlist')

		print alertgenlist

		# sys.exit()

		if alertgenlist is None:
			break

		if len(alertgenlist) == 0:
			break

		col = alertgenlist.pop()

		mc.set('alertgenlist',[])
		mc.set('alertgenlist',alertgenlist)


		command = "~/anaconda2/bin/python %s %s && ~/anaconda2/bin/python %s %s" % (c.fileio.alertgenscript,col,c.fileio.alertanalysisscript,col)
        
		if lockscript.get_lock('alertgen for %s' % col,exitifexist=False):
			p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
		else:
			continue

		print "Appending ", col
		ongoing.append(p)

		if len(ongoing) < proc_limit:
			continue
		else:
			while len(ongoing) >= proc_limit:
				for i in reversed(ongoing):
					alive = i.poll()
					if alive is not None:
						ongoing.remove(i)
						print "Popped process", i
						break



if __name__ == "__main__":
    main()