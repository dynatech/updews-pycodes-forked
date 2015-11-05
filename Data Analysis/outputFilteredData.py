import querySenslopeDb as qs
import filterSensorData as fs

#targetTable = 'bolb'
node = 9
sensors = qs.GetSensorDF()
#print sensors

for s in range(len(sensors)):
    targetTable = sensors.name[s]
    df = qs.GetRawAccelData(siteid = targetTable, fromTime = "2015-10-15", toTime = "2015-10-25", msgid = 33, targetnode = node)
    numElements = len(df.index)
    #print "Number of %s Raw elements: %s" % (targetTable, numElements)
    qs.PrintOut("Number of %s Raw elements: %s" % (targetTable, numElements))
    
    if numElements > 0:
        df_filtered = fs.applyFilters(df, orthof=True, rangef=True, outlierf=True)
        numFiltered = len(df_filtered.index)
        
        if numFiltered > 0:
            qs.PrintOut("Number of %s filtered elements: %s" % (targetTable, numFiltered))
            #print df_filtered
        else:
            print "No valid filtered data for %s" % (targetTable)
