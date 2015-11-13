import pandas as pd
import querySenslopeDb as qs
import filterSensorData as fs
import sys

#Get filtered data for all sites
def getFilteredAll():
    try:
        start = sys.argv[1]
    except IndexError:
        start = ''
        
    try:
        end = sys.argv[2] 
    except IndexError:
        end = ''     
        
    try:
        msgid = sys.argv[3] 
    except IndexError:
        msgid = 32
    
    sensors = qs.GetSensorDF()
    
    for s in range(len(sensors)):
        targetTable = sensors.name[s]
        df = qs.GetRawAccelData(siteid = targetTable, fromTime = start, toTime = end, msgid = msgid)
        numElements = len(df.index)
        qs.PrintOut("Number of %s Raw elements: %s" % (targetTable, numElements))
        
        if numElements > 0:
            df_filtered = fs.applyFilters(df, orthof=True, rangef=True, outlierf=True)
            numFiltered = len(df_filtered.index)
            
            if numFiltered > 0:
                qs.PrintOut("Number of %s filtered elements: %s" % (targetTable, numFiltered))
                #print df_filtered
            else:
                print "No valid filtered data for %s" % (targetTable)

#get filtered data for a selected site
#let us set "nil" for the "empty value" received from PHP's GET
def getFilteredData(isCmd = True, inSite = "", inNode = 1, inStart = "", inEnd = "", inMsgid = 32):
    if isCmd == True:
        try: #site selection
            site = sys.argv[1]
        except IndexError:
            print "No site has been selected. Script unable to run!"
            return
            
        try: #node selection
            node = sys.argv[2]
    
            if node == 'nil':
                node = -1
        except IndexError:
            node = -1
    
        try: #start date
            start = sys.argv[3]
    
            if start == 'nil':
                start = ''
        except IndexError:
            start = ''
            
        try: #end date
            end = sys.argv[4] 
    
            if end == 'nil':
                end = ''
        except IndexError:
            end = ''       
            
        try: #switch between accel 1 and 2
            msgid = sys.argv[5] 
    
            if msgid == 'nil':
                msgid = 32
        except IndexError:
            msgid = 32
    else:
        site = inSite
        node = inNode
        start = inStart
        end = inEnd
        msgid = inMsgid

    #print "variables: %s %s %s %s %s" % (site,node,start,end,msgid)

    df = qs.GetRawAccelData(siteid = site, fromTime = start, toTime = end, msgid = msgid, targetnode = node)
    numElements = len(df.index)
    qs.PrintOut("Number of %s Raw elements: %s" % (site, numElements))
    
    if numElements > 0:
        df_filtered = fs.applyFilters(df, orthof=True, rangef=True, outlierf=False)
        numFiltered = len(df_filtered.index)
        
        if numFiltered > 0:
            qs.PrintOut("Number of %s filtered elements: %s" % (site, numFiltered))
            return df_filtered
        else:
            qs.PrintOut("No valid filtered data for %s" % (site))
            return pd.DataFrame()
    
    #return empty dataframe
    return pd.DataFrame()

def getFilteredDataJSON():
    dfa = getFilteredData()
    isDFempty = dfa.empty
    
    if isDFempty == True:
        print 'No Data Available...'
    else:
        dfajson = dfa.to_json(orient="records",date_format='iso')
        dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
        print dfajson