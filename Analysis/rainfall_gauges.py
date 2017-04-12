from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import requests
import numpy as np

import querydb as q

def to_MySQL(df, engine):
    gauge_name = df['gauge_name'].values[0]
    try:
        df.to_sql(name = 'rainfall_gauges', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
#        print gauge_name, ': success'
    except:
        query = "SELECT * FROM %s WHERE gauge_name = '%s'" %('rainfall_gauges', gauge_name)
        rain_id = q.GetDBDataFrame(query).rain_id[0]
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE %s SET latitude = %s, longitude = %s WHERE rain_id = %s" %('rainfall_gauges', df['latitude'].values[0], df['longitude'].values[0], rain_id)
        cur.execute(query)
        db.commit()
        db.close()
#        print gauge_name, ': updated'

def main():
    r = requests.get('http://weather.asti.dost.gov.ph/web-api/index.php/api/devices', auth=('phivolcs.ggrdd', 'PhiVolcs0117'))    
    noah = pd.DataFrame(r.json())
    noah = noah[noah['sensor_name'].str.contains('rain', case = False)]
    noah = noah.dropna()
    noah['dev_id'] = noah['dev_id'].apply(lambda x: int(x))
    noah['longitude'] = noah['longitude'].apply(lambda x: np.round(float(x),6))
    noah['latitude'] = noah['latitude'].apply(lambda x: np.round(float(x),6))
    noah = noah.loc[(noah.longitude != 0) & (noah.latitude != 0)]
    noah = noah.rename(columns = {'dev_id': 'gauge_name'})
    noah['data_source'] = 'noah'
    noah = noah[['gauge_name', 'data_source', 'longitude', 'latitude']]
    noah_id = noah.groupby('gauge_name')
    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
    noah_id.apply(to_MySQL, engine=engine)

################################################################################
if __name__ == "__main__":
    start = datetime.now()
    main()
    print 'runtime =', datetime.now() - start