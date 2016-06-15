"""
Created on Mon Jun 13 11:08:15 2016

@author: PradoArturo
"""

#!/usr/bin/python		
# This is server.py file

# Import socket module
import socket

# Create a socket object
s = socket.socket()
# Get local machine name
host = socket.gethostname()
port = 5051
s.bind((host, port))

s.listen(5)
while True:
	c, addr = s.accept()
	print 'Got connection from', addr
	c.send('Thank you for connecting')
	#c.close()
	msg = c.recv(4096)
	print msg
