import socket
import os
import sys
import time
import json
import simplejson
import pandas as pd
from datetime import datetime


#Show the schemas available from the masynckaiser server
#Note: even though this information is sent to the client, the schema 
#   permissions from the masynckaiser_schema_targets table will still apply
def showSchemas():
    request = """{"dir":0,"action":"read","query":"show databases"}"""
    return request

#Show the tables from the schema of interest
#Note: even though this information is sent to the client, the table 
#   permissions from the masynckaiser_table_permissions table will still apply
def showTables(schema):
    request = """{"dir":0,"action":"read","query":"show tables",
                  "schema":"%s"}""" % (schema)
    return request