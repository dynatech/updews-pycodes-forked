import sys
from analysis.subsurface import vcdgen as vcd
    
def getDF():
    
    site_column = sys.argv[1]
    end_ts = sys.argv[2].replace("n",'').replace("T"," ").replace("%20"," ")
    start_ts = sys.argv[3].replace("n",'').replace("T"," ").replace("%20"," ")

#    site_column = "agbta"
#    end_ts = "2017-11-11 06:00:00"
#    start_ts = "2017-11-08 06:00:00"
    
    df = vcd.vcdgen(site_column, end_ts, start_ts) 
    print df

getDF()