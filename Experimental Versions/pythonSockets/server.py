"""
Created on Mon Jun 13 11:08:15 2016

@author: PradoArturo
"""

#!/usr/bin/python		
# This is server.py file

# Import socket module
import socket
import time

# Create a socket object
s = socket.socket()
# Get local machine name
host = socket.gethostname()
port = 5051
s.bind((host, port))

s.listen(5)
#while True:
#	c, addr = s.accept()
#	text = 'Got connection from', addr
#	print text
#	c.send('Thank you for connecting')
#	#c.close()
#	msg = c.recv(1024)
#	print msg
##	c.send("server received msg: %s" % msg)


while True:
    c, addr = s.accept()
    text = 'Got connection from', addr
    print text
    c.send('Thank you for connecting')
    
    # continuously send a spam of messages to client
    ctr = 10
    delay = 20
    while ctr > 0:
        time.sleep(delay) # delays for "delay" seconds
        msg = "DateTime " + time.strftime("%c")
        print msg
        c.send(msg)
    
    c.close()
    
    
    
    
    
    
    
    
    