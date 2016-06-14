# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 13:40:37 2016

@author: PradoArturo
"""

import csv
import sys
import json
import re
import datetime
import glob, os
import pandas as pd

#include the path of "Inhouse Library" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../Inhouse Library'))
if not path in sys.path:
    sys.path.insert(1,path)
del path  

import updateLocalDbLib as uldb

def removeDuplicates():
#Delete duplicates
#    query = "ALTER IGNORE TABLE gndmeas ADD UNIQUE INDEX "
#    query += "idx_name (timestamp,meas_type,site_id,crack_id)"

    #Add a primary key "id" for table "gndmeas"
    print "Adding temporary primary key for easier deletion of duplicates..."
    query = "ALTER TABLE `gndmeas` "
    query += "ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT FIRST, "
    query += "ADD PRIMARY KEY (`id`)"
    print query
    uldb.ExecuteQuery(query)

    #Delete duplicates except for one
    print "Deleting duplicates..."
    query = "DELETE gnd1 "
    query += "FROM  gndmeas gnd1, gndmeas gnd2 "
    query += "WHERE gnd1.timestamp = gnd2.timestamp "
    query += "AND gnd1.meas_type = gnd2.meas_type "
    query += "AND gnd1.site_id = gnd2.site_id "
    query += "AND gnd1.crack_id = gnd2.crack_id "
    query += "AND gnd1.id <> gnd2.id "
    print query
    uldb.ExecuteQuery(query)

    #Remove primary key "id"
    print "Removing temporary primary key..."
    query = "ALTER TABLE gndmeas "
    query += "DROP COLUMN `id`, "
    query += "DROP PRIMARY KEY"
    print query
    uldb.ExecuteQuery(query)

    #Make "timestamp, meas_type, site_id, crack_id" the primary keys 
    print "Make timestamp, meas_type, site_id, crack_id the primary keys..."
    query = "ALTER TABLE gndmeas "
    query += "ADD PRIMARY KEY (`timestamp`, `meas_type`, `site_id`, `crack_id`)"
    print query
    uldb.ExecuteQuery(query)

def readGroundMeasurementsCSV (myFile):
    with open(myFile,'rb') as f:
        reader = csv.reader(f)
        
        #header = ['timestamp','site','feature','N','measure','reliable','feat desc','weather','observer']    
        header = ['timestamp','meas_type','site_id','crack_id','observer_name','meas','weather']        
        df = pd.DataFrame(columns=(header))
        isStart = True
        
        rownum = 0
        macronum = 0
        for row in reader:
            if isStart:
#                header = row
#                df = pd.DataFrame(columns=(header))
                isStart = False
                continue

            temp = []

            #Organize row before pushing to dataframe
            temp.append(row[0])         #timestamp
            
            temp.append("ROUTINE")      #meas_type
            
            #site_id
            if (row[2] == None):
                continue       
            else:
                temp.append(row[1][0:3].upper())
            
            #crack_id
            if (row[2] == None):
                continue       
            else:
                temp.append(row[2].title())
            
            #observer_name
            if (row[8] == None):
                temp.append("Unavailable")         
            else:
                temp.append(row[8].title())
                
            #meas   
            if (row[4] == None):
                continue
            else:
                if len(row[4]) <= 8:
                    meas = float(row[4])
                    if meas < 2000:
                        temp.append(row[4])                
            
            #weather
            if (row[7] == None):
                temp.append("Unavailable")         
            else:
                temp.append(row[7][0:31].lower())

            try:
                df.loc[rownum] = temp
            except ValueError:
                print "mismatched columns: %s + %s" % (macronum, rownum)
            
            rownum += 1
            
            if (rownum % 100 == 0):
                print "inserting macronum: %s" % (macronum)                
                
                df = df.set_index(['timestamp','site_id','crack_id'])
                uldb.writeDFtoLocalDB("gndmeas", df)
                
                df = []
                df = pd.DataFrame(columns=(header))
                rownum = 0
                macronum += 1
                
            

#Go through all the csv file and import them to the database
for file in glob.glob("*.csv"):
    print(file)
    root = os.path.dirname(os.path.realpath(__file__))
    fullPath = os.path.join(root, file)

    readGroundMeasurementsCSV(fullPath)
    removeDuplicates()










        
            
            