# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 09:22:33 2016

@author: Kevin Dhale dela Cruz

Updates the corresponding Rainfall Senslope tables of each sites
if the site has available Rainfall Senslope values.

"""

from sqlalchemy import *
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy import Table, MetaData, orm
from sqlalchemy.orm import sessionmaker, mapper

import requests

# Database credentials
host = 'localhost'
user = 'root'
password = 'dyn4m1ght'
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
    

def getRainfallSenslopeTables(conn, meta):
    """
    Retrieves all Rainfall Senslope tables for each site;
    Returns the columns 'name' and 'rain_senslope' from table 'site_rain_props'
    as an array of lists
    
    """
    table = Table('site_rain_props', meta, autoload=True)
    query = (
        select([ table.c.name, table.c.rain_senslope ], 
               table.c.rain_senslope != null)
        .distinct()
        .group_by(table.c.rain_senslope)
    )
    result = conn.execute(query).fetchall()
    #print result
    
    rainsenslope_array = []
    for row in result:
        rainsenslope_array.append(row)
    
    #print arq_array
    return rainsenslope_array


def createTableIfDoesNotExist(senslope_table, meta):
    """
    Checks if a rainfall table exists and creates a table
    if it does not exists
    
    """
    table = Table(senslope_table, meta, 
                Column('timestamp', DATETIME(), primary_key=True, nullable=False), 
                Column('name', CHAR(length=4), primary_key=True, nullable=False), 
                Column('temp', DOUBLE(asdecimal=True)), 
                Column('wspd', INTEGER(display_width=11)), 
                Column('wdir', INTEGER(display_width=11)), 
                Column('rain', DOUBLE(asdecimal=True)), 
                Column('batt', DOUBLE(asdecimal=True)), 
                Column('csq', INTEGER(display_width=11)), 
                schema=None, autoload=True)
    
    return table
    
    
def getLastTimestamp(meta, senslope_table):
    """
    Gets a Rainfall Senslope table on local database's last timestamp
    to serve as starting point for update and download
    of new data on DEWS Landslide server
    
    """    
    session = createSession()
    
    # Check if the rainfall table exists
    if not db.has_table(senslope_table):
        print "Database does not have table {0}.".format(senslope_table)
        print "Creating table {0}".format(senslope_table)
        table = createTableIfDoesNotExist(senslope_table, meta)
        result[0] = "2010-01-01 00:00:00"
    else:
        table = Table(senslope_table, meta, autoload=True)    
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
    url = ('http://www.dewslandslide.com/ajax/getSenslopeData.php/?rainsenslope&site={0}&start_date={1}&limit={2}').format(site, date, entryLimit)
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
    Maps Rainfall Senslope table to be updated to a Python object for easier
    manipulation and saving
    
    """
    class Data(object):
        pass

    mapper(Data, table)
    data = Data()
    return data
    

def updateSenslopeTable(senslopeTable, data):
    """
    Updates an Rainfall Senslope table if the database value
    
    """
    session = createSession()
    table = meta.tables[senslopeTable]
    
    for row in data:
        #print row
        obj = dataTableMapper(table)
        obj.__table_name__ = senslopeTable
        
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

# Get the corresponding Rainfall Senslope tables for site if available
rainsenslope_tables = getRainfallSenslopeTables(conn, meta)

# Iterate to all Rainfall Senslope tables and update each
for row in rainsenslope_tables:
    while downloadMore is True:    
        # Get the latest timestamp
        last_timestamp = getLastTimestamp(meta, row[1])
        
        # Get data from DEWS API
        data = getDataFromDEWSapi(row[0], last_timestamp)
        try:
            print "Data retrieved: {0}".format(len(data['rain_senslope']))
        except:
            print "Data retrieved: 0"
        
        # If data is NULL, the local database is updated and will exit
        if not data['rain_senslope']:
            print "Rainfall Senslope table '{0}' is up-to-date.\n".format(row[1])
            break
        # Else update table
        else:
            updateSenslopeTable(row[1], data['rain_senslope'])        
            print "Succesfully updated Rainfall Senslope table '{0}'.\n".format(row[1])




