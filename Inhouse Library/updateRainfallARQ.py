# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 09:22:33 2016

@author: Kevin Dhale dela Cruz

Updates the corresponding Rainfall ARQ tables of each sites
if the site has available ARQ values.

"""

from sqlalchemy import *
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy import Table, MetaData, orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper

import requests

# Database credentials
host = 'localhost'
user = 'root'
password = 'senslope'
database = 'senslopedb'

# Number of row to be retrieved from API per call
entryLimit = 600

##################
#   Functions
##################

def createSession():
    Session = sessionmaker(bind=db)
    session = Session()
    return session
    

def getRainfallARQTables(conn, meta):
    """
    Retrieves all Rainfall ARQ tables for each site;
    Returns the columns 'name' and 'rain_arq' from table 'site_rain_props'
    as an array of lists
    
    """
    table = Table('site_rain_props', meta, autoload=True)
    query = (
        select([ table.c.name, table.c.rain_arq ], 
               table.c.rain_arq != null)
        .distinct()
        .group_by(table.c.rain_arq)
    )
    result = conn.execute(query).fetchall()
    #print result
    
    arq_array = []
    for row in result:
        arq_array.append(row)
    
    #print arq_array
    return arq_array
    
    
def getLastTimestamp(meta, arq_table):
    """
    Gets an ARQ table on local database's last timestamp
    to serve as starting point for update and download
    of new data on DEWS Landslide server
    
    """    
    session = createSession()
    table = Table(arq_table, meta, autoload=True)
    result = (
        session.query(table.c.timestamp)
        .order_by(table.c.timestamp.desc())
        .first()
    )
    session.close()
    
    #print result[0]
    return result[0]
    

def getDataFromDEWSapi(site, date):
    """
    Retrieves data from DEWS website API    
    
    """
    url = ('http://www.dewslandslide.com/ajax/getSenslopeData.php/?rainarq&site={0}&start_date={1}&limit={2}').format(site, date, entryLimit)
    print("Fetching URL... {0} Done.").format(url)
    
    response = requests.get(url)

    if response.status_code != 200:
        print('Status: {0}, Problem with the request. Exiting program.').format(response.status_code)
        exit()
    else:
        print('Status: {0}, Success!\n').format(response.status_code)
    
    data = response.json()
    #print data
    return data


def dataTableMapper(table):
    """
    Maps ARQ table to be updated to a Python object for easier
    manipulation and saving
    
    """
    class Data(object):
        pass

    mapper(Data, table)
    data = Data()
    return data
    

def updateARQTable(arqTable, data):
    """
    Updates an ARQ table if the database value
    
    """
    session = createSession()
    table = meta.tables[arqTable]
    
    for row in data:
        #print row
        obj = dataTableMapper(table)
        obj.__table_name__ = arqTable
        
        for key, value in row.iteritems():
            setattr(obj, key, value)
        #print obj.timestamp
        session.add(obj)
    session.commit()
    
    
#####################
##  Main Program
#####################

# Connecting to database
url = URL('mysql', user, password, host, None, database)
db_url = make_url(url)
db = create_engine(db_url)
db.echo = False # Turning this to True will show SQLAlchemy SQL logs
conn = db.connect()

# Create a database map of tables to be used on the program
meta = MetaData(bind=db)

downloadMore = True

# Get the corresponding ARQ tables for site if available
arq_tables = getRainfallARQTables(conn, meta)

# Iterate to all ARQ tables and update each
for row in arq_tables:
    while downloadMore is True:
        # Get the latest timestamp
        last_timestamp = getLastTimestamp(meta, row[1])
        #print "Last timestamp: ", last_timestamp    
        
        # Get data from DEWS API
        data = getDataFromDEWSapi(row[0], last_timestamp)
        try:
            print "Data retrieved: {0}".format(len(data))
        except:
            print "Data retrieved: 0"
        
        # If data is NULL, the local database is updated and will exit
        if data is None:
            print "ARQ table '{0}' is up-to-date.\n".format(row[1])
            break
        # Else update table
        else:
            updateARQTable(row[1], data)
            print "Succesfully updated ARQ table '{0}'.\n".format(row[1])




