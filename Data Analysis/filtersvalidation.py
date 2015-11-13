# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 16:24:57 2015

@author: Mizpah
"""

import querySenslopeDb as qs
import filterSensorData as fs


sensors = qs.GetSensorDF()
#print sensors

for s in range(len(sensors)):
    targetTable = sensors.name[s]

    #df = qs.GetRawAccelData(siteid = targetTable, fromTime = "2013-01-01", msgid = 33)
    df = qs.GetRawAccelData(siteid = targetTable, fromTime = "2013-01-01")
    numElements = len(df.index)

        
    if numElements > 0:
        df_resampled = fs.applyFilters(df, orthof=True, rangef=True, outlierf=False)
        df_filtered = fs.applyFilters(df, orthof=False, rangef=False, outlierf=True)
        numFiltered= len(df_filtered.index)
        
        drawcountx = df_resampled.x.count()
        drawcounty = df_resampled.y.count()
        drawcountz = df_resampled.z.count()
        dfinalcountx = df_filtered.x.count()
        dfinalcounty = df_filtered.y.count()
        dfinalcountz = df_filtered.z.count()
            
        if numFiltered > 0:
            qs.PrintOut("Data Count Summary for %s" %(targetTable))
            qs.PrintOut("Raw Data (resampled without pad)")
            qs.PrintOut("Xraw: %s" % (drawcountx))
            qs.PrintOut("Yraw: %s" % (drawcounty))
            qs.PrintOut("Zraw: %s" % (drawcountz))
            qs.PrintOut("Filtered Data")
            qs.PrintOut("Xf: %s" % (dfinalcountx))
            qs.PrintOut("Yf: %s" % (dfinalcounty))
            qs.PrintOut("Zf: %s" % (dfinalcountz))


           #print df_filtered
        else:
            print "No valid filtered data for %s" % (targetTable)
