# -*- coding: utf-8 -*-
"""
Created on Mon Oct 03 17:44:15 2016

@author: PradoArturo
"""

from datetime import datetime
import os
import sys

#include the path of "libraries" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../libraries'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import masynckaiserWSSLib as masync

host = "www.dewslandslide.com"
#host = "sandbox"
port = 5055
schema = "senslopedb"

if len(sys.argv) >= 2:
    table = sys.argv[1]
    print "UPDATING TABLE: %s" % (table)
    
    # Call this function to update selected table and sync the data from the client
    #   machine to the Masync Web Socket Server (dewslandslide.com)
    masync.interfaceUpdateTableOfWSS(host, port, schema, table)
else:
    print """%s: Needs only 1 argument, the name of the table to be updated on 
        the server side""" % (__file__)