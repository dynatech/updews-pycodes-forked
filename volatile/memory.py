import memcache

def get_handle():
	print "Connecting to memcache client ...",
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	print "done"
	return mc

def print_config(cfg = None):
	mc = get_handle()

	sc = mc.get('server_config')

	if not cfg:
		for key in sc.keys():
			print key, sc[key], "\n"
	else:
		print cfg, sc[cfg]

def server_config():
	mc = get_handle()
	return mc.get("server_config")