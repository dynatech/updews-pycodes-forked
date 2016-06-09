import socket, time, sys

def get_lock(process_name):
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print process_name, 'has lock'
    except socket.error:
        print process_name, 'lock exists'
        print 'aborting'
        sys.exit()