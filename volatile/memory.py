import memcache

def get_handle(print_out = False):
	if print_out:
		print "Connecting to memcache client ...",
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	if print_out:
		print "done"
	return mc

def get(name=""):
	mc = get_handle()
	return mc.get(name)

def set(name="",data=""):
	mc = get_handle()
	return mc.set(name,data)

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