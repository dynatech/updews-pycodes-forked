#!/usr/bin/python

import socket

s = socket.socket()
#host = socket.gethostname()
host = "www.codesword.com"
port = 5051

s.connect((host, port))
print s.recv(1024)
s.send("This is a test message")
s.close()
