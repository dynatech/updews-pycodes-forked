# -*- coding: utf-8 -*-
"""
Created on Mon Mar 21 11:08:15 2016

@author: PradoArturo
"""

import ConvertSomsRaw as soms
import querySenslopeDb as qs
import pandas as pd

#column = raw_input('Enter column name: ')
#gid = int(raw_input('Enter id: '))
fdate = "2016-01-01 00:00:00"
tdate = "2016-03-20"

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
                
                test = soms.getsomsrawdata(columnName, node, fdate, tdate)
                #print test
                print "row count: %s" % (len(test.index))
                
                pass

except IndexError:
    print '>> Error in writing extracting database data to files..'

#test = soms.getsomsrawdata(column, gid, fdate, tdate)
#print test