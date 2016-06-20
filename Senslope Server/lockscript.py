import socket, time, sys

def get_lock(process_name):
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print process_name, 'process does not exist. Proceeding... '
    except socket.error:
        print process_name, 'process exists. Aborting...'
        print 'aborting'
        sys.exit()
