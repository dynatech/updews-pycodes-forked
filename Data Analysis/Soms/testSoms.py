# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 11:08:15 2016

@author: PradoArturo
"""

import os
import sys
import time
import pandas as pd
import ConvertSomsRaw as soms

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

#SOMS Inputs should be:
#   a. column
#   b. nid
#   c. start date
#   d. end date

try:
    db, cur = qs.SenslopeDBConnect('senslopedb')
    print '>> Connected to database'

    #Get all column names with SOMS
    queryColumns = 'SHOW TABLES LIKE "%m"'
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
            queryNodes = 'SELECT DISTINCT id FROM %s WHERE id > 0 AND id < 40 ORDER BY id' % (columnName)
            cur.execute(queryNodes)
            
            nodes = cur.fetchall()
            print nodes
        
            for nodeData in nodes:
                node = nodeData[0]
                print "%s: %s" % (columnName, node)
                
#                test = soms.getsomscaldata(columnName, node, fdate, tdate)
                test = soms.getsomsrawdata(columnName, node, fdate, tdate)
                #print test
                print "row count: %s" % (len(test.index))
                
                pass

except IndexError:
    print '>> Error in writing extracting database data to files..'

#test = soms.getsomsrawdata(column, gid, fdate, tdate)
#print test