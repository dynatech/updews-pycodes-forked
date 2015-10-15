import urllib
import urllib2

## This will download CSV files that contain 14 days worth of data
def getrain():
    ## BLC
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
    print "downloading BLC"
    urllib.urlretrieve(url, "ASTI/blcw.csv")
    
    ## BOL
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1236'
    print "downloading BOL"
    urllib.urlretrieve(url, "ASTI/bolw.csv")
    
    ## GAM
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=782'
    print "downloading GAM"
    urllib.urlretrieve(url, "ASTI/gamw.csv")
    
    ## HUM
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=789'
    print "downloading HUM"
    urllib.urlretrieve(url, "ASTI/humw.csv")
    
    ## LAB
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=389'
    print "downloading LAB"
    urllib.urlretrieve(url, "ASTI/labw.csv")
    
    ## LIP
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1236'
    print "downloading LIP"
    urllib.urlretrieve(url, "ASTI/lipw.csv")
    
    ## MAM
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=389'
    print "downloading MAM"
    urllib.urlretrieve(url, "ASTI/mamw.csv")
    
    ## OSL
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=152'
    print "downloading OSL"
    urllib.urlretrieve(url, "ASTI/oslw.csv")
    
    ## PLA
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=789'
    print "downloading PLA"
    urllib.urlretrieve(url, "ASTI/plaw.csv")
    
    ## PUG
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=65'
    print "downloading PUG"
    urllib.urlretrieve(url, "ASTI/pugw.csv")
    
    ## SIN
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=454'
    print "downloading SIN"
    urllib.urlretrieve(url, "ASTI/sinw.csv")
    
    ## ANT
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=467'
    print "downloading SAG"
    urllib.urlretrieve(url, "ASTI/antw.csv")
    
    ## TUE
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=469'
    print "downloading TUE"
    urllib.urlretrieve(url, "ASTI/tuew.csv")
    
    ## CUD
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=391'
    print "downloading CUD"
    urllib.urlretrieve(url, "ASTI/cudw.csv")

    ## 02. Baretto -> Abucay (Device ID: 1103)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1103'
    print "downloading Baretto"
    urllib.urlretrieve(url, "ASTI//barw.csv")
    
    ## 04. Dadong -> Tarragona (Device ID: 733)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=733'
    print "downloading Dadong"
    urllib.urlretrieve(url, "ASTI//dadw.csv")
    
    ## 05. Sibahay -> Brgy. Cabasagan, Boston (Device ID: 1450)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1450'
    print "downloading Sibahay"
    urllib.urlretrieve(url, "ASTI//sibw.csv")
    
    ## 06. Agbatuan-> Brgy. Rapulang, Maayon (Device ID: 557)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=557'
    print "downloading Agbatuan"
    urllib.urlretrieve(url, "ASTI//agbw.csv")
    
    ## 07. Bayabas -> CNSC, Jose Panganiban (Device ID: 79)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=79'
    print "downloading Bayabas"
    urllib.urlretrieve(url, "ASTI//bayw.csv")
    
    ## 08. Lunas -> PSTC SOUTHERN LEYTE, MAASIN (Device ID: 89)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=89'
    print "downloading Lunas"
    urllib.urlretrieve(url, "ASTI//lunw.csv")
    
    ## 10. Sumalsag -> ARCH BRIDGE, MALITBOG (Device ID: 760)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=760'
    print "downloading Sumalsag"
    urllib.urlretrieve(url, "ASTI//sumw.csv")
    
    ## 11. Magsaysay -> Dangcagan (Device ID: 867)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=867'
    print "downloading Magsaysay"
    urllib.urlretrieve(url, "ASTI//magw.csv")
    
    ## 12. McArthur -> DON FLAVIA, SAN LUIS (Device ID: 607
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=607'
    print "downloading McArthur"
    urllib.urlretrieve(url, "ASTI//macw.csv")
    
    ## 14. Pitu -> SULOP, POBLACION, SULOP (Device ID: 363)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=363'
    print "downloading Pitu"
    urllib.urlretrieve(url, "ASTI//pitw.csv")
    
    ## 15. Kanaan -> MDRRM OFFICE, IGACOS (Device ID: 1459)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1459'
    print "downloading Kanaan"
    urllib.urlretrieve(url, "ASTI//kanw.csv")
    
    ## 16. Sto. Nino -> Talaingod, Davao del Norte (Device ID: 858)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=858'
    print "downloading Nino"
    urllib.urlretrieve(url, "ASTI//ninw.csv")
    
    ## 17. Monte Duali -> Laak, Davao del Norte (Device ID: 1289)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1289'
    print "downloading Monte"
    urllib.urlretrieve(url, "ASTI//monw.csv")
    
    ## 18. SanCarlos -> Siargao Island, Surigao del Norte (Device ID: 180)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=180'
    print "downloading SanCarlos"
    urllib.urlretrieve(url, "ASTI//carw.csv")
    
    ## 19. Nurcia -> Carmen, Surigao del Sur (Device ID: 1561)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1561'
    print "downloading Nurcia"
    urllib.urlretrieve(url, "ASTI//nurw.csv")
    
    ## 20. Inabasan -> Maasin, Iloilo (Device ID: 289)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=289'
    print "downloading Inabasan"
    urllib.urlretrieve(url, "ASTI//inaw.csv")
    
    ## 21. Umingan -> Alimodian, Iloilo (Device ID: 204)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
    print "downloading Umingan"
    urllib.urlretrieve(url, "ASTI//umiw.csv")
    
    ## 22. Pepe -> Alimodian, Iloilo (Device ID: 204)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
    print "downloading Pepe"
    urllib.urlretrieve(url, "ASTI//pepw.csv")
    
    ## 23. Marirong -> Alimodian, Iloilo (Device ID: 204)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
    print "downloading Marirong"
    urllib.urlretrieve(url, "ASTI//marw.csv")
    
    ## 24. Pinagkamaligan -> Brgy. Villahermosa, Quezon (Device ID: 1096)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1096'
    print "downloading Pinagkamaligan"
    urllib.urlretrieve(url, "ASTI//pinw.csv")
	
    ## 25. Pizarro -> Gandara Bridge, Samar (Device ID: 550)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=550'
    print "downloading Pizarro"
    urllib.urlretrieve(url, "ASTI//pizw.csv")

	## 26. Poblacion I -> Gandara Bridge, Samar (Device ID: 550)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=550'
    print "downloading Pizarro"
    urllib.urlretrieve(url, "ASTI//pobw.csv")

	## 27. Lipata -> Paranas Municipal Hall, Samar (Device ID: 619)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=619'
    print "downloading Lipata"
    urllib.urlretrieve(url, "ASTI//ataw.csv")

	## 28. Literon -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
    print "downloading Literon"
    urllib.urlretrieve(url, "ASTI//litw.csv")
	
	## 29. Laygayon -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
    print "downloading Laygayon"
    urllib.urlretrieve(url, "ASTI//layw.csv")
	
	## 30. Parasanon -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
    print "downloading Parasanon"
    urllib.urlretrieve(url, "ASTI//parw.csv")	
	
	## 31. Manghulyawon -> South Poblacion, Ayungon, Negros Oriental (Device ID: 681)
    url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=681'
    print "downloading Manghulyawon"
    urllib.urlretrieve(url, "ASTI//manw.csv")		
	
	
	
