import socket
import sys


def get_lock(process_name,exitifexist=True):
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print (process_name, 'process does not exist. Proceeding... ')
        return True
    except socket.error:
        print (process_name, 'process exists. Aborting...')
        print ('aborting')
        if exitifexist:
        	sys.exit()
        else:
        	return False
