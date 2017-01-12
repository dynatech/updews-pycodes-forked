from datetime import datetime, timedelta
import pandas as pd

import AllRainfall as A
import querySenslopeDb as q

def main(ts='', site=''):
    # data timestamp
    if ts == '':
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
            site_rain_props = rain_props[rain_props.name == site]
            if len(site_rain_props) != 0:
                break
            print 'site not in the list'
    else:
        site_rain_props = rain_props[rain_props.name == site]
    # 3-day threshold
    twoyrmax = site_rain_props['max_rain_2year'].values[0]
    # 1-day threshold
    halfmax = twoyrmax/2.
    gauge_ids = [site_rain_props['RG1'].values[0]] +[site_rain_props['RG2'].values[0]] +[site_rain_props['RG3'].values[0]]
    try:
        if site_rain_props['rain_senslope'].values[0] != None:
            gauge_ids = [site_rain_props['rain_senslope'].values[0]] + gauge_ids
    except:
        pass
    try:
        if site_rain_props['rain_arq'].values[0] != None:
            gauge_ids = [site_rain_props['rain_arq'].values[0]] + gauge_ids
    except:
        pass
    # rain gauge id
    while True:
        gauge = raw_input('rain gauge id (e.g. agbtaw or 557): ').lower()
        try:
            gauge = 'rain_noah_' + str(int(gauge))
        except:
            pass
        if gauge in gauge_ids:
            break
        print '\n\n'
        print 'rain gauge id not in the list'
        rain_lst = '##  ' + ','.join(gauge_ids) + '  ##'
        print '#'*len(rain_lst)
        print rain_lst
        print '#'*len(rain_lst)
        print '\n\n'
        
    # rainfall data for the past 3 days
    start = ts - timedelta(3) - timedelta(hours=0.5)
    end = ts + timedelta(hours=0.5)
    rainfall = A.GetResampledData(gauge, start, end)
    rainfall = rainfall[(rainfall.index >= ts - timedelta(3))&(rainfall.index <= ts)]

    try:
        one,three = A.onethree_val_writer(rainfall)
        return ts, site, one, halfmax, three, twoyrmax, gauge_ids, gauge
    except:
        one, three = '', ''
        return ts, site, one, halfmax, three, twoyrmax, gauge_ids, gauge

###############################################################################

if __name__ == "__main__":
    start_time = datetime.now()
    ts, site, one, halfmax, three, twoyrmax, gauge_ids, gauge = main()
    gauge_lst = gauge_ids
    while gauge in gauge_lst:
        gauge_lst.remove(gauge)
        if len(gauge_lst) == 0 and one == '':
            break
        elif one == '':
            note_data = "##  no data choose other rain gauge  ##"
            print '\n\n\n'
            print '#' * len(note_data)
            print note_data
            print '#' * len(note_data)
            print '\n\n\n'
            ts, site, one, halfmax, three, twoyrmax, gauge_ids, gauge = main(ts=ts, site=site)
        else:
            break
    if one != '':
        one_cml = '1-day: ' + str(one) + ' mm (threshold: ' + str(halfmax) + ')'
        three_cml = '3-day: ' + str(three) + ' mm (threshold: ' + str(twoyrmax) + ')'
        space = len(three_cml) - len(one_cml)
        print '\n\n\n'
        print '#'*(max(len(one_cml), len(three_cml))+8)
        if len(three_cml) == len(one_cml):
            print '##  ' + one_cml + '  ##'
            print '##  ' + three_cml + '  ##'
        elif len(three_cml) > len(one_cml):
            print '##  ' + one_cml + ' '*space + '  ##'
            print '##  ' + three_cml + '  ##'
        else:
            print '##  ' + one_cml + '  ##'
            print '##  ' + three_cml + ' '*space + '  ##'
        print '#'*(max(len(one_cml), len(three_cml))+8)
        print '\n\n\n'
    else:
        note = "##  No data for all rain gauges  ##"
        print '\n\n\n'
        print '#'*len(note)
        print note
        print '#'*len(note)
        print '\n\n\n'
    print "runtime = ", datetime.now()-start_time