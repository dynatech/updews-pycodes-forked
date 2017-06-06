# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 14:58:19 2017

@author: kennex
"""

import pandas as pd
import configfileio as cfg
config = cfg.config()
import platform
curOS = platform.system()
if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

    
def GetPiezoConstants(logger_name):
    db = mysqlDriver.connect(host = config.dbio.hostdb, user = config.dbio.userdb, passwd = config.dbio.passdb)
    cur = db.cursor()
    cur.execute("USE {}".format(config.dbio.namedb))
    query = """
    select C0kpa,C1kpa,C2kpa,C3kpa,C4kpa,C5kpa from piezo_sensors
    inner join loggers on
    loggers.logger_id = piezo_sensors.logger_id
    where logger_name = '{}'
    """.format(logger_name)
    cur.execute(query)
#    print query
    return cur.fetchall()[0]

def FreqToPressure(df):
    logger_name=df.name.iloc[0]

    if len(logger_name) > 6:    
        logger_name = logger_name[0:5]
        print logger_name
    else:
        logger_name = logger_name[0:4]
        print logger_name
    c = GetPiezoConstants(logger_name)
    df['kPa'] = float(c[0]) + (float(c[1])*df.freq) + (float(c[2])*df.temp) + (float(c[3])*df.freq*df.freq) + (float(c[4])*df.freq*df.temp) + (float(c[5])*df.temp*df.temp)
    return df
    
df = pd.read_csv('ltesapzpz.csv')
a = FreqToPressure(df)
