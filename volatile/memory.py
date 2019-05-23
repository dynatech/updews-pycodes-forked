import memcache


def get_handle(print_out = False):
	"""
	- Description.

	Args:
	    Args (str): Args.

	Returns:
	    Returns.

	Raises:
	    Raise.

	"""

	if print_out:
		print ("Connecting to memcache client ...",)
	mc = memcache.Client(['127.0.0.1:11211'],debug=1)
	if print_out:
		print ("done")
	return mc


def get(name=""):
	"""
	- Description.

	Args:
	    Args (str): Args.

	Returns:
	    Returns.

	Raises:
	    Raise.

	"""  
	name = name.upper()
	mc = get_handle()
	return mc.get(name)


def set(name="", value=""):
	"""
	- Description.

	Args:
	    Args (str): Args.

	Returns:
	    Returns.

	Raises:
	    Raise.

	"""  

	name = name.upper()
	mc = get_handle()
	return mc.set(name,value)


def delete(name=""):
	"""
	- Description.

	Args:
	    Args (str): Args.

	Returns:
	    Returns.

	Raises:
	    Raise.

	"""  
    
	name = name.upper()
	mc = get_handle()
	mc.delete(name)
	print ("Delete successfully " + name)


def print_config(cfg = None):
	"""
	- Description.

	Args:
	    Args (str): Args.

	Returns:
	    Returns.

	Raises:
	    Raise.

	"""  
	mc = get_handle()

	sc = mc.get('server_config')

	if not cfg:
		for key in sc.keys():
			print (key, sc[key], "\n")
	else:
		print (cfg, sc[cfg])


def server_config():
	"""
	- Description.

	Args:
	    Args (str): Args.

	Returns:
	    Returns.

	Raises:
	    Raise.

	"""  
	return get("server_config")

