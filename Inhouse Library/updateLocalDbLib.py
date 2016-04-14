# -*- coding: utf-8 -*-
"""
Created on Tue Jun 02 10:20:04 2015

@author: chocolate server
"""

import urllib
import urllib2
import datetime
import time
import json
import pandas as pd
import MySQLdb, ConfigParser
from StringIO import StringIO
from MySQLdb import OperationalError
from pandas.io import sql
from pandas.io.json import json_normalize

dbname = "senslopedb"
dbhost = "127.0.0.1"
dbuser = "root"
dbpwd = "senslope"
#dbuser = "updews"
#dbpwd = "october50sites"
entryLimit = 600

###############################################################################
# Read from Database
###############################################################################

def getLatestTimestamp(col):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "select max(timestamp) from %s.%s" % (dbname, col)
    ret = 0
    try:
        a = cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = 0
    finally:
        dbc.close()
        print ret
        return ret
        
def getLatestNodeStatusId():
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "select max(post_id) from %s.node_status" % (dbname)
    ret = 0
    try:
        a = cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = 0
    finally:
        dbc.close()
        return ret
        
def getSiteColumnsList():
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "select name from %s.site_column where s_id < 100 order by name asc" % (dbname)

    df = pd.read_sql(query, con=dbc)
    #print df
    print "Number of loaded records: ", len(df)
    dbc.close()
    return df
    
def getSiteSomsList():
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "select name from %s.site_column where s_id < 100 and CHAR_LENGTH(name) = 5 and SUBSTRING(name, 4, 1) = 's' order by name asc;" % (dbname)

    df = pd.read_sql(query, con=dbc)
    #print df
    print "Number of loaded records: ", len(df)
    dbc.close()
    return df
        
def checkEntryExistence(table,col,value):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "select %s from %s.%s where %s = '%s'" % (col,dbname,table,col,value)
    #print query
    ret = 0
    try:
        a = cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = 0
    finally:
        dbc.close()
        return ret
        
def checkTableExistence(table):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "select 1 from %s.%s limit 1" % (dbname,table)
    print query
    ret = 1
    try:
        a = cur.execute(query)
        ret = 1
    except:
        print "Table doesn't exist"
        ret = -1
    finally:
        dbc.close()
        return ret
    
###############################################################################
# Create Tables on Database    
###############################################################################
        
def createSensorTable(table,version):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    
    if version == 1:
        query = "CREATE TABLE `%s`.`%s` (`timestamp` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',`id` INT(11) NOT NULL DEFAULT '0',`xvalue` INT(11) NULL DEFAULT NULL,`yvalue` INT(11) NULL DEFAULT NULL,`zvalue` INT(11) NULL DEFAULT NULL,`mvalue` INT(11) NULL DEFAULT NULL,PRIMARY KEY (`id`, `timestamp`)) ENGINE = InnoDB DEFAULT CHARACTER SET = latin1;" % (dbname,table)
    elif version == 2:
        query = "CREATE TABLE `%s`.`%s` (`timestamp` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',`id` INT(11) NOT NULL DEFAULT '0',`msgid` SMALLINT(6) NOT NULL DEFAULT '0',`xvalue` INT(11) NULL DEFAULT NULL,`yvalue` INT(11) NULL DEFAULT NULL,`zvalue` INT(11) NULL DEFAULT NULL,`batt` DOUBLE NULL DEFAULT NULL, PRIMARY KEY (`id`, `msgid`,`timestamp`)) ENGINE = InnoDB DEFAULT CHARACTER SET = latin1;" % (dbname,table)
    elif version == 3:
        query = "CREATE TABLE `%s`.`%s` (`timestamp` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',`id` INT(11) NOT NULL DEFAULT '0',`msgid` SMALLINT(6) NOT NULL DEFAULT '0',`xvalue` INT(11) NULL DEFAULT NULL,`yvalue` INT(11) NULL DEFAULT NULL,`zvalue` INT(11) NULL DEFAULT NULL,`batt` DOUBLE NULL DEFAULT NULL, PRIMARY KEY (`id`, `msgid`,`timestamp`)) ENGINE = InnoDB DEFAULT CHARACTER SET = latin1;" % (dbname,table)    
    elif version == "soms":
        query = "CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, msgid smallint, mval1 int, mval2 int, PRIMARY KEY (timestamp, id, msgid))" %table
    
    #print query
    print "Created sensor table version %s: '%s'" % (version,table)
    ret = 0
    
    try:
        a = cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = 0
    finally:
        dbc.close()
        return ret      

def createNodeStatusTable():
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()

    query = "CREATE TABLE `%s`.`node_status` (`post_id` BIGINT(20) NOT NULL AUTO_INCREMENT, `post_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `date_of_identification` DATE NULL, `flagger` VARCHAR(45) NOT NULL, `site` VARCHAR(8) NOT NULL, `node` INT(11) NOT NULL, `status` VARCHAR(32) NOT NULL, `comment` VARCHAR(255) NULL, `inUse` TINYINT(1) NOT NULL, PRIMARY KEY (`post_id`)) ENGINE = InnoDB DEFAULT CHARACTER SET = latin1;" % (dbname)
    
    #print query
    print "Created Node Status Table: node_status"
    ret = 0
    
    try:
        a = cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = 0
    finally:
        dbc.close()
        return ret   
        
###############################################################################
# Truncate Tables on Database    
###############################################################################        
        
def truncateTable(col):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    cur = dbc.cursor()
    query = "SET foreign_key_checks = 0; TRUNCATE TABLE %s.%s;" % (dbname, col)
    ret = 0
    try:
        a = cur.execute(query)
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = 0
    finally:
        dbc.close()
        print ret
        return ret            
        
###############################################################################
# Download data from the dewslandslide web API        
###############################################################################        
        
def downloadLatestData(col,fromDate='',toDate=''):
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&accelsite&site=%s&limit=%s&start=%s&end=%s' % (dbname,col,entryLimit,fromDate,toDate)
    print url
    print "Downloading", col, "data from", fromDate, "...",
    
    # comment out this part for direct (no proxy) connection
    # urllib2.install_opener(
        # urllib2.build_opener(
            # urllib2.ProxyHandler({'http': 'proxy.engg.upd.edu.ph:8080'})
        # )
    # )    
    
    try:
        f = urllib2.urlopen(url);
        s = f.read().strip()
        #print s
        conv = StringIO(s)
        df = pd.DataFrame.from_csv(conv, sep=',', parse_dates=False)
        print "downloadLatestData done"
        return df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"

def downloadFullSiteColumnTable():
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&sitecolumnjson' % (dbname)     
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        df = df.set_index(['s_id'])
        print "downloadFullSiteColumnTable done" 
        #print df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        #return empty data frame
        return pd.DataFrame()
    
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        #return empty data frame
        return pd.DataFrame()
        
    return df
    
def downloadFullColumnPropsTable():
    #url = 'http://localhost/temp/getSenslopeData.php?db=%s&columninfojson' % (dbname)
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&columninfojson' % (dbname)
    #url = 'http://www.dewslandslide.com'    
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        df = df.set_index(['s_id'])
        print "downloadFullColumnPropsTable done" 
        #print df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        #return empty data frame
        return pd.DataFrame()
    
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        #return empty data frame
        return pd.DataFrame()
        
    return df

def downloadFullRainPropsTable():
    #url = 'http://localhost/temp/getSenslopeData.php?db=%s&columninfojson' % (dbname)
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&raininfojson' % (dbname)
    #url = 'http://www.dewslandslide.com'    
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        df = df.set_index(['name'])
        print "downloadFullColumnPropsTable done" 
        #print df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        #return empty data frame
        return pd.DataFrame()
    
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        #return empty data frame
        return pd.DataFrame()
        
    return df

def downloadSoilMoistureTablesList():
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&sitesomsjson' % (dbname)     
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        df = df.set_index(['name'])
        print "downloadSoilMoistureTablesList done" 
        #print df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        #return empty data frame
        return pd.DataFrame()
    
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        #return empty data frame
        return pd.DataFrame()
        
    return df
    
def downloadSoilMoistureData(col,fromDate='',toDate=''):
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&sitesomsdata&site=%s&limit=%s&start=%s&end=%s' % (dbname,col,entryLimit,fromDate,toDate)    
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        df = df.set_index(['timestamp'])
        print "downloadSoilMoistureData done" 
        #print df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        #return empty data frame
        return pd.DataFrame()
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        #return empty data frame
        return pd.DataFrame()
    except:
        print "returned null"
        return pd.DataFrame()
        
    return df

def downloadSiteColumnData(col):
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&singlesitecolumn&name=%s' % (dbname,col)
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        #df.columns = ['s_id','name','date_install','date_activation','lat','long','sitio','barangay','municipality','province','region','loc_desc','affected_households','installation_status','version']
        #df = df.set_index(['s_id'])
        #print df.iloc[0]['name']
        newSite = df.iloc[0]['name']
        version = df.iloc[0]['version']
        print "downloadSiteColumnData done" 
        
        exists = checkEntryExistence("site_column","name",newSite)
        if exists == 0:
            #create the new table
            print "%s site not found in site_column... adding it to table" % (newSite)
            createSensorTable(newSite,version)
        else:
            #create the new table
            print "%s site already exists in site_column" % (newSite)
            createSensorTable(newSite,version)

        #return df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        return None
    
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        return None
        
    return True
    
def downloadNodeStatusData(postId = 0):
    #check existence of node_status table
    exists = checkTableExistence("node_status")
    if exists < 0:
        #create the new table
        print "node_status table is not found in database"
        createNodeStatusTable()
        latestPid = postId
    else:
        latestPid = getLatestNodeStatusId()
        
    print "latest post id = %s" % latestPid
    
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&nodestatus&pid=%s&json' % (dbname,latestPid)  
    print url
    
    try:
        jsonData = pd.read_json(url, orient='columns')
        df = pd.DataFrame(jsonData)
        df = df.set_index(['post_id'])
        print "downloadNodeStatusData done" 
        
        #return df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"
        return pd.DataFrame()
    
    except ValueError:
        print "No Data from web server. Table does not exist on web server"
        return pd.DataFrame()
        
    return df
   
###############################################################################
# Writes to Database
###############################################################################   
   
# Append Write
def writeDFtoLocalDB(col,df,fromDate='',numDays=30):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    print "writing df to", col, "db",
    df.to_sql(con=dbc, name=col, if_exists='append', flavor='mysql')
    dbc.close()

    print "writeDFtoLocalDB done"   

# Overwrite (Used mostly on static info updating)
def overwriteDFtoLocalDB(col,df):   
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    print "writing df to", col, "db",

    try:
        df.to_sql(con=dbc, name=col, if_exists='replace', flavor='mysql')
        dbc.close()
    except OperationalError as e:
        if 'MySQL server has gone away' in str(e):
            print 'write failed...'
            time.sleep(3)
        else:
            raise e()

    print "overwriteDFtoLocalDB done"    

def writeAccelToLocalDB(col,df,fromDate='',numDays=30):
    dbc = MySQLdb.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
    print "writing df to", col, "db",

    try:
        df.to_sql(con=dbc, name=col, if_exists='append', flavor='mysql')
        dbc.close()
    except OperationalError as e:
        if 'MySQL server has gone away' in str(e):
            #limit the number of days to reduce size of update data
            print 'reconnecting and trying again...'
            
            date_1 = datetime.datetime.strptime(fromDate, "%Y-%m-%d %H:%M:%S")
            end_date = date_1 + datetime.timedelta(days=numDays)
            toDate = end_date.strftime("%Y-%m-%d %H:%M:%S")

            df = downloadLatestData(col,fromDate,toDate)
            writeAccelToLocalDB(col,df,fromDate,numDays/2)
            print e
            
            time.sleep(3)
        else:
            raise e()

    print "writeAccelToLocalDB done"

###############################################################################

#update the site tables for accelerometer data
def updateAccelData():
    sensors = getSiteColumnsList()    
    
    for index, row in sensors.iterrows():
        col = row['name']
        #print "current column is: %s" % (col)
        downloadMore = True
        while downloadMore:
            print col
            ts = getLatestTimestamp(col)
            if ts == 0:
                # auto generate tables that don't exist in the database of the local 
                # machine running the script
                print 'There is no table named: ' + col
                existingOnWebServer = downloadSiteColumnData(col)
                ts2 = "2000-01-01+00:00:00"
       
                if existingOnWebServer == None:
                    downloadMore = False
                    
                continue
            elif ts == None:
                ts2 = "2000-01-01+00:00:00"
            else:
                ts2 = ts.strftime("%Y-%m-%d+%H:%M:%S")
            
            df = downloadLatestData(col,ts2)
            
            try:
                numElements = len(df.index)
                print "Number of dataframe elements: %s" % (numElements)
                #print df
                writeAccelToLocalDB(col,df,ts2)
        
                if numElements < entryLimit:
                    downloadMore = False     
            except:
                print "No additional data downloaded for %s" % (col)
                downloadMore = False
 

#update the soms tables for soil moisture data
def updateSomsData():
    somsList = getSiteSomsList()
    somsListWeb = downloadSoilMoistureTablesList()
    
    print "Local List"
    for index, row in somsList.iterrows():
        soms = row['name'] + 'm'
        #print soms
        
        if soms in somsListWeb.index:
            print "%s exists on the web!!!" % (soms)

            ts = getLatestTimestamp(soms)
            if ts == 0:
                print 'There is no table named: ' + soms
                createSensorTable(soms,"soms")
                ts2 = "2000-01-01+00:00:00"
                continue
            elif ts == None:
            	ts2 = "2000-01-01+00:00:00"
            else:
            	ts2 = ts.strftime("%Y-%m-%d+%H:%M:%S")
             
            df = downloadSoilMoistureData(soms,ts2)
            print df
'''
    		ts = getLatestTimestamp(col)
    		if ts == 0:
            	# auto generate tables that don't exist in the database of the local 
             	# machine running the script
    			print 'There is no table named: ' + col
    			existingOnWebServer = downloadSiteColumnData(col)
    			ts2 = "2000-01-01+00:00:00"
       
    			if existingOnWebServer == None:
        				downloadMore = False
    				
    			continue
    		elif ts == None:
    			ts2 = "2000-01-01+00:00:00"
    		else:
    			ts2 = ts.strftime("%Y-%m-%d+%H:%M:%S")
            
    		df = downloadLatestData(col,ts2)
    		numElements = len(df.index)
    		print "Number of dataframe elements: %s" % (numElements)
    		#print df
    		writeAccelToLocalDB(col,df,ts2)
    
    		if numElements < entryLimit:
    			downloadMore = False  
'''



#update the site_column data
def updateSiteColumnTable():
    df = downloadFullSiteColumnTable()
    isDFempty = df.empty
    targetTable = "site_column"      

    if isDFempty == True:
        print 'Update failed...'
    else:
        print 'Updating %s table...' % (targetTable)
        numElements = len(df.index)
        print "Number of %s elements: %s" % (targetTable, numElements)
          
        truncateTable(targetTable)
        time.sleep(2)
        writeDFtoLocalDB(targetTable,df)

#update the site_column_props data
def updateColumnPropsTable():
    df = downloadFullColumnPropsTable()
    isDFempty = df.empty
    targetTable = "site_column_props"
    
    if isDFempty == True:
        print 'Update failed...'
    else:
        print 'Updating %s table...' % (targetTable)
        numElements = len(df.index)
        print "Number of %s elements: %s" % (targetTable, numElements)
          
        truncateTable(targetTable)
        time.sleep(2)
        writeDFtoLocalDB(targetTable,df)
        
#update the node_status data
def updateNodeStatusTable():
    df = downloadNodeStatusData()    
    isDFempty = df.empty
    targetTable = "node_status"
    
    if isDFempty == True:
        print 'Update failed...'
    else:
        print 'Updating %s table...' % (targetTable)
        numElements = len(df.index)
        print "Number of %s elements: %s" % (targetTable, numElements)
          
        writeDFtoLocalDB(targetTable,df)    
        
#update the site_rain_props data
def updateRainPropsTable():
    df = downloadFullRainPropsTable()
    isDFempty = df.empty
    targetTable = "site_rain_props"
    
    if isDFempty == True:
        print 'Update failed...'
    else:
        print 'Updating %s table...' % (targetTable)
        numElements = len(df.index)
        print "Number of %s elements: %s" % (targetTable, numElements)
          
        truncateTable(targetTable)
        time.sleep(2)
        writeDFtoLocalDB(targetTable,df)