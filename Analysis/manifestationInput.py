#reaading input system for MoMs
import pandas as pd
from sqlalchemy import create_engine

import querySenslopeDb as qdb


def get_manifestations_gdoc():
    cols=['ts_report','ts_observation','site_code','reporter','feature','feature_name','report','validator','remarks','alert']
    url = 'https://docs.google.com/spreadsheets/d/1bfSgBOHZHaDz6r7tWpADiywCG6muTZ9aoKIKeXSvzG8/export?format=csv'
    df = pd.read_csv(url,usecols=range(1,len(cols)+1),parse_dates=[0,1,2],names=cols,skiprows=[0])
    
    return df

def get_manifestations_db(table_name):
    query = 'SELECT * FROM %s.%s' % (qdb.Namedb, table_name)
    df = qdb.GetDBDataFrame(query)
    df = df.drop('report_id',axis=1)
    return df

def push_to_db(df,table_name):
    Userdb = qdb.Userdb
    Passdb = qdb.Passdb
    Hostdb = qdb.Hostdb
    Namedb = qdb.Namedb
    
    engine = create_engine('mysql://'+Userdb+':'+Passdb+'@'+Hostdb+':3306/'+Namedb)
    
    try:
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = Namedb,index=False)
    except IOError:
        print 'error occurred'

def push_alerts(df):
    adf = pd.DataFrame(columns=['timestamp','site','source','alert','updateTS'])
    adf.timestamp = df.ts_observation
    adf.site = df.site_code.str.lower()
    adf.source = 'moms'
    adf.alert = df.alert.str.upper()
    adf.updateTS = df.ts_observation
    
    push_to_db(adf,'site_level_alert')
    
    return adf
    

def create_manifestations_table():
   query =  "CREATE TABLE `senslopedb`.`manifestations` (\n"
   query += "`report_id` INT NOT NULL AUTO_INCREMENT,\n"
   query += "`ts_report` DATETIME DEFAULT '2010-01-01 00:00:00',\n"
   query += "`ts_observation` DATETIME NOT NULL,\n"
   query += "`site_code` VARCHAR(45) NOT NULL,\n"
   query += "`reporter` VARCHAR(45) NOT NULL,\n"
   query += "`feature` VARCHAR(45) NOT NULL,\n"
   query += "`feature_name` VARCHAR(45) NOT NULL,\n"
   query += "`report` TEXT NOT NULL,\n"
   query += "`validator` VARCHAR(4) NOT NULL,\n"
   query += "`remarks` TEXT NOT NULL,\n"
   query += "`alert` VARCHAR(2) NOT NULL,\n"
   query += "PRIMARY KEY (`report_id`))"
   print 'table not found. creating table... \n'
   qdb.ExecuteQuery(query)

def main():
    exists = qdb.DoesTableExist('manifestations')
    
    if not exists:
        create_manifestations_table()        

    gdoc = get_manifestations_gdoc()
    db = get_manifestations_db('manifestations')
    new = pd.concat([db,gdoc]).drop_duplicates(keep=False)
        
    if len(new) != 0:
        print 'pushing new features!'
        push_to_db(new,'manifestations')
        push_alerts(new)
        
    else:
        print 'no new features to push!'

if __name__ == '__main__':
    main()