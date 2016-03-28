# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 11:08:15 2016

@author: PradoArturo
"""

import os
import sys
import time
import pandas as pd

#TODO: Add the accelerometer filter module you need to test
#import newAccelFilter as naf

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs

#column = raw_input('Enter column name: ')
#gid = int(raw_input('Enter id: '))
#fdate = raw_input('Enter Start Date: ')
#tdate = raw_input('Enter End Date: ')

fdate = "2015-01-01 00:00:00"
tdate = time.strftime("%Y-%m-%d %H:%M")

try:
    db, cur = qs.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'

    #Get all column names with installation status of "Installed"
    queryColumns = 'SELECT name, version FROM site_column WHERE installation_status = "Installed" ORDER BY s_id ASC'
    try:
        cur.execute(queryColumns)
    except:
        print '>> Error parsing database'
    
    columns = cur.fetchall()
    print columns

    for column in columns:
        columnName = column[0]
        if len(columnName) <= 6:
            #Get list of nodes for column
            queryNodes = 'SELECT DISTINCT id FROM %s WHERE id > 0 AND id < 60 ORDER BY id' % (columnName)
            cur.execute(queryNodes)
            
            nodes = cur.fetchall()
        
            for nodeData in nodes:
                node = nodeData[0]
                print "%s: %s" % (columnName, node)
                   
#                Accel Inputs should be:
#                   a. column
#                   b. nid
#                   c. version
#                   d. start date
#                   e. end date
                   
#                TODO: add your accelerometer filter here 
#                test = naf.newAccelFilterFxn(columnName, node, version, fdate, tdate)
#                print test
#                print "row count: %s" % (len(test.index))                
                
                pass

except IndexError:
    print '>> Error in writing extracting database data to files..'