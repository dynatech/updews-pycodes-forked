from datetime import datetime
import pandas as pd
import numpy as np
import querySenslopeDb as q
from sqlalchemy import create_engine

def to_MySQL(df, table_name):
    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
    try:
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        print 'success'
    except:
        try:
            db, cur = q.SenslopeDBConnect(q.Namedb)
            if table_name == 'rain_gauge':
                query = "DELETE FROM %s WHERE dev_id = '%s'" %(table_name, df['dev_id'].values[0])
            elif table_name == 'rain_props':
                query = "DELETE FROM %s WHERE name = '%s'" %(table_name, df['name'].values[0])
            cur.execute(query)
            db.commit()
            db.close()
            df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = q.Namedb, index = False)
            print 'updated'
        except:
            print 'error'

def updateDB():
    NOAHRG = pd.read_json('http://weather.asti.dost.gov.ph/home/index.php/api/devices')
    NOAHRG = NOAHRG[NOAHRG['sensor_name'].str.contains('rain', case = False)]
    NOAHRG = NOAHRG.loc[(NOAHRG.posx != 0) & (NOAHRG.posy != 0)]
    NOAHRG = NOAHRG[['dev_id', 'posx', 'posy', 'location', 'province']]
    id_NOAHRG = NOAHRG.groupby('dev_id')
    id_NOAHRG.apply(to_MySQL, table_name = 'rain_gauge')

################################################################################

def SiteCoord():
    RGdf = q.GetRainProps()
    RGdf = RGdf.loc[RGdf.name != 'msl']
    
    RG = list(RGdf.rain_arq.dropna().apply(lambda x: x[:len(x)-1]))
    RG = '|'.join(RG)
    query = "SELECT * FROM senslopedb.site_column where name REGEXP '%s'" %RG
    RGCoord = q.GetDBDataFrame(query)
    RGCoord['name'] = RGCoord.name.apply(lambda x: x + 'w')

    RG = list(RGdf.rain_senslope.dropna().apply(lambda x: x[:len(x)-1]))
    RG = '|'.join(RG)
    query = "SELECT * FROM senslopedb.site_column where name REGEXP '%s'" %RG
    df = q.GetDBDataFrame(query)
    df['name'] = df.name.apply(lambda x: x[0:3] + 'w')
    RGCoord = RGCoord.append(df)
    
    query = "SELECT * FROM senslopedb.site_column where name = 'loo'"
    df = q.GetDBDataFrame(query)
    RGCoord = RGCoord.append(df)
    RGCoord = RGCoord.drop_duplicates(['sitio', 'barangay', 'municipality', 'province'])
    RGCoord = RGCoord[['name', 'lat', 'lon', 'barangay', 'province']]    
    RGCoord = RGCoord.rename(columns = {'name': 'dev_id', 'barangay': 'location'})
    RGCoord['type'] = 'SenslopeRG'
    RGCoord = RGCoord.sort('dev_id')
    return RGCoord

def NOAHRGCoord():
    db, cur = q.SenslopeDBConnect(q.Namedb)
    query = "SELECT * FROM senslopedb.rain_gauge"
    RGCoord = q.GetDBDataFrame(query)
    RGCoord['dev_id'] = RGCoord.dev_id.apply(lambda x: 'rain_noah_' + str(x))
    RGCoord = RGCoord.rename(columns = {'posx': 'lat', 'posy': 'lon'})
    RGCoord['type'] = 'NOAHRG'
    return RGCoord
    
def AllRGCoord():
    SenslopeCoord = SiteCoord()
    SenslopeCoord = SenslopeCoord.loc[SenslopeCoord.dev_id != 'loo']
    NOAHCoord = NOAHRGCoord()
    RGCoord = SenslopeCoord.append(NOAHCoord)
    return RGCoord
    
def Distance(name):
    Coord = SiteCoord()
    lat = Coord.loc[Coord.dev_id == name].lat.values[0]
    lon = Coord.loc[Coord.dev_id == name].lon.values[0]
    
    NearGauge = AllRGCoord()
    
    NearGauge['dlat'] = NearGauge.lat - lat
    NearGauge['dlon'] = NearGauge.lon - lon
    NearGauge['dlat'] = np.radians(NearGauge.dlat)
    NearGauge['dlon'] = np.radians(NearGauge.dlon)
    
    NearGauge['a'] = (np.sin(NearGauge.dlat/2))**2 + ( np.cos(np.radians(lat)) * np.cos(np.radians(NearGauge.lat)) * (np.sin(NearGauge.dlon/2))**2 )
    NearGauge['c']= 2 * np.arctan2(np.sqrt(NearGauge.a),np.sqrt(1-NearGauge.a))
    NearGauge['d']= 6371 * NearGauge.c
    NearGauge = NearGauge.drop(['a','c','dlon','dlat'], axis=1)
#    NearGauge = NearGauge.loc[NearGauge.d <= 20]
    NearGauge = NearGauge.sort('d', ascending = True)
    NearGauge = NearGauge.loc[NearGauge.dev_id != name]
    
    return NearGauge[0:3]

def NearRGdf(df):
    print df['name'].values[0]
    if df['name'].values[0] == 'loo':
        d = Distance('loo')
    else:
        try:
            d = Distance(df['rain_arq'].values[0])
        except:
            d = Distance(df['rain_senslope'].values[0])
    
    df['RG1'] = d['dev_id'].values[0]
    df['RG2'] = d['dev_id'].values[1]
    df['RG3'] = d['dev_id'].values[2]

    to_MySQL(df, 'rain_props')

    return df

def main():
    RGdf = q.GetRainProps()[['name', 'max_rain_2year', 'rain_senslope', 'rain_arq']]
    siteRGdf = RGdf.groupby('name')
    RG = siteRGdf.apply(NearRGdf)
    return RG
    
if __name__ == "__main__":
    start = datetime.now()
    
    updateDB()
    main()

    print 'runtime =', datetime.now() - start