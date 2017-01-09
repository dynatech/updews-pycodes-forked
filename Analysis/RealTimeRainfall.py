from datetime import datetime, timedelta
import pandas as pd

import AllRainfall as A
import querySenslopeDb as q

def main(ts=datetime.now(), site=''):
    # data timestamp
    if ts == datetime.now():
        while True:
            try:
                ts = pd.to_datetime(raw_input('timestamp format YYYY-MM-DD HH:MM (e.g. 2017-01-13 19:30): '))
                break
            except:
                print 'invalid timestamp format'
                pass
    rain_props = q.GetRainProps('rain_props')
    # rain gauge properties
    if site == '':
        while True:
            site = raw_input('site (e.g. agb): ').lower()
            rain_props = rain_props[rain_props.name == site]
            if len(rain_props) != 0:
                break
            print 'site not in the list'
    else:
        rain_props = rain_props[rain_props.name == site]
    # 3-day threshold
    twoyrmax = rain_props['max_rain_2year'].values[0]
    # 1-day threshold
    halfmax = twoyrmax/2.
    gauge_ids = [rain_props['RG1'].values[0]] +[rain_props['RG2'].values[0]] +[rain_props['RG3'].values[0]]
    try:
        if rain_props['rain_senslope'].values[0] != None:
            gauge_ids += [rain_props['rain_senslope'].values[0]]
    except:
        pass
    try:
        if rain_props['rain_arq'].values[0] != None:
            gauge_ids += [rain_props['rain_arq'].values[0]]
    except:
        pass
    # rain gauge id
    while True:
        gauge = raw_input('rain gauge id (e.g. agbtaw or 557): ')
        try:
            gauge = 'rain_noah_' + str(int(gauge))
        except:
            pass
        if gauge in gauge_ids:
            break
        print 'rain gauge id not in the list'
        print ','.join(gauge_ids)
        
    # rainfall data for the past 3 days
    start = ts - timedelta(3) - timedelta(hours=0.5)
    end = ts + timedelta(hours=0.5)
    rainfall = A.GetResampledData(gauge, start, end)
    rainfall = rainfall[(rainfall.index >= ts - timedelta(3))&(rainfall.index <= ts)]

    try:
        one,three = A.onethree_val_writer(rainfall)
        return ts, site, one, halfmax, three, twoyrmax
    except:
        one, three = '', ''
        return ts, site, one, halfmax, three, twoyrmax

###############################################################################

if __name__ == "__main__":
    start_time = datetime.now()
    ts, site, one, halfmax, three, twoyrmax = main()
    while True:
        if one == '':
            print '\n\n\n\n\n'
            print "no data choose other rain gauge"
            print '\n\n\n\n\n'
            ts, site, one, halfmax, three, twoyrmax = main(ts=ts, site=site)
        else:
            break
    print '\n\n\n\n\n'
    print '1-day:', one, 'mm ( threshold:', halfmax, ')'
    print '3-day:', three, 'mm ( threshold:', twoyrmax, ')'
    print '\n\n\n\n\n'
    print "runtime = ", datetime.now()-start_time