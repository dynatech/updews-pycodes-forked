import os,time,re
import MySQLdb
import datetime
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta as tda
import pandas as pd
import numpy as np
from querySenslopeDb import *

configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

section = "File I/O"

MFP = cfg.get(section,'MachineFilePath')
PurgedFP = MFP + cfg.get(section,'PurgedFilePath')
MonPurgedFP = MFP + cfg.get(section,'PurgedMonitoringFilePath')
LastGoodDataFP = MFP + cfg.get(section,'LastGoodDataFilePath')
cutoff = float(cfg.get('Value Limits', 'cutoff'))
moninterval = cfg.getint('Value Limits', 'moninterval')

window = cfg.get('Filter Args', 'window')
order = cfg.get('Filter Args', 'order')
off_lim = cfg.get('Filter Args', 'off_lim')

def ConvertToDf(data):
    # convert each row to list
    data = [list(row) for row in data]
    # do the conversion
    df = pd.DataFrame.from_records(data, coerce_float=True, columns=['ts','id','x','y','z','m'])
    # make the timestamp column the index of the df
    # df = df.set_index('ts')

    return df

def PurgeNonAlignedEntries(df):
    # select accelerometer values only
    dfa = df[['x','y','z']]
    # normalize
    dfa_2 = pow(dfa.iloc[:]/1024.0,2)
    # raise all values to 2
    dfa_dp = pow(dfa_2.x + dfa_2.y + dfa_2.z, 0.5)
    # find vector lengths with range of 1-cutoff<vl<1+cutoff
    return df.loc[(dfa_dp>1.0-cutoff) & (dfa_dp < 1.0+cutoff)]

def LimitSOMSdata(df):

    # select out of range data and replace will NULL value
    upLim = 4000
    lowLim = 2000
    df.m[df.m < lowLim]= np.nan
    df.m[df.m > upLim] = np.nan

    return df
    

def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    import numpy as np
    from math import factorial

    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError, msg:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y.iloc[0] - np.abs( y[1:half_window+1][::-1] - y.iloc[0] )
    lv = len(y.index)-1
    lastvals = y.iloc[lv] + np.abs(y[-half_window-1:-1][::-1] - y.iloc[lv])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')

def AdjustOffsetInAxis(ax):
    window = 101
    order = 3
    off_lim = 80

    # sanity check

    if len(ax) < window:
        print 'short series'
        return ax

    f = savitzky_golay(ax, window, order)
    # adjust high offset
    ax[ax-f>off_lim] = ax[ax-f>off_lim] - 128
    # adjust low offset
    ax[f-ax>off_lim] = ax[f-ax>off_lim] + 128

    return ax

def FixOneBitChange(df):

    dft = df.copy()

    # cycle through all unique node ids.
    for nid in df.id.unique():
        # select series with nid
        print repr(nid),

        dft[dft.id == nid].x = AdjustOffsetInAxis(dft.x)
        dft[dft.id == nid].y = AdjustOffsetInAxis(dft.y)
        dft[dft.id == nid].z = AdjustOffsetInAxis(dft.z)

    return dft

def GenerateLastGoodData(site, df):

    # create new DataFrame for last good data
    dflgd = pd.DataFrame()
    # cycle through all unique node ids.
    for nid in np.sort(df.id.unique()):
        lat = df[df.id == nid].iloc[-1]
        dflgd = dflgd.append(lat)
        
    # add missing nodes in lgd 
    for nid in range(1, site.nos + 1):
        if nid not in df.id.unique():
            new = dflgd.iloc[0].copy()
            new.x = 1023
            new.y = 0
            new.z = 0
            new.id = nid
            new.m = np.nan

            dflgd = dflgd.append(new)

    # reformat dataframe (because sh**)
    dflgd = dflgd[['ts','id','x','y','z','m']]

    return dflgd
	
def GenPurgedFiles():
    print 'Generating purged files:'

    sites = GetSensorList()

    for site in sites:

        siteid = site.name
        print siteid, 

        data = GetRawColumnData(siteid)
        df = ConvertToDf(data)
        df = PurgeNonAlignedEntries(df)

        if siteid=='sinb':
            df = FixOneBitChange(df)

        
        if siteid!='pugt' and siteid!='pugb':
            df = LimitSOMSdata(df)
            
        df.to_csv(PurgedFP + siteid + ".csv", index=False, header=False)

        dflgd = GenerateLastGoodData(site,df)
        dflgd.to_csv(LastGoodDataFP + siteid + ".csv", index=False, header=False, float_format='%.0f')

        print 'done'

def GenerateMonitoringPurgedFiles():
    print 'Generating purged files:'

	# get a list of columnArray classes
    sites = GetSensorList()

    for site in sites:
        siteid = site.name
        print siteid, 

        ft = dt.now()- tda(days=moninterval)
        data = GetRawColumnData(siteid, ft.strftime("%Y-%m-%d %H:%M:%S"))
        df = ConvertToDf(data)
        df = PurgeNonAlignedEntries(df)

        if siteid=='sinb':
            df = FixOneBitChange(df)

        dflgd = pd.read_csv(LastGoodDataFP + siteid + ".csv", names=['ts','id','x','y','z','m'])

        # get the missing node data from last good data file
        df = df.append(dflgd.loc[~dflgd.id.isin(df.id.unique())])
        
        df.to_csv(MonPurgedFP + siteid + ".csv", index=False, header=False)
        print 'done'


##def main():
##
##    GeneratePurgedFiles()'
##    
##if __name__ == '__main__':
##    main()
  



