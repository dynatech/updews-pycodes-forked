import querySenslopeDb as qs
import filterSensorData as fs

#targetTable = 'bolb'
sensors = qs.GetSensorDF()
#print sensors

for s in range(len(sensors)):
    targetTable = sensors.name[s]
    df = qs.GetRawAccelData(siteid = targetTable, fromTime = "2015-10-15", msgid = 33)
    numElements = len(df.index)
    print "Number of %s Raw elements: %s" % (targetTable, numElements)
    
    if numElements > 0:
        df_filtered = fs.applyFilters(df, orthof=True, rangef=True, outlierf=True)
        numElements = len(df_filtered.index)
        print "Number of %s filtered elements: %s" % (targetTable, numElements)
