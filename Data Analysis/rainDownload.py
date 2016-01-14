import urllib
import urllib2

## This will download CSV files that contain 14 days worth of data
def getrain(site, gauge_num):

    if site == 'agbtaw':
        if gauge_num == 1:
            ## 01. Agbatuan-> Tapulang, Maayon, Capiz (Device ID: 557)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=557'
            print "downloading Agbatuan"
            urllib.urlretrieve(url, "ASTI//agbtaw.csv")
            
        elif gauge_num == 2:
            ## 01. Agbatuan-> Plaza, San Rafael, Iloilo (Device ID: 1255)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1255'
            print "downloading Agbatuan2"
            urllib.urlretrieve(url, "ASTI//agbtaw.csv")
            
        else:
            ## 01. Agbatuan-> Plaza, Lemery, Iloilo (Device ID: 1258)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1258'
            print "downloading Agbatuan3"
            urllib.urlretrieve(url, "ASTI//agbtaw.csv")
        
    if site == 'bakw':
        if gauge_num == 1:
            ## 39. Bakun -> Bakun, Benguet (Device ID: 450)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=450'
            print "downloading Bakun"
            urllib.urlretrieve(url, "ASTI//bakw.csv")
            
        elif gauge_num == 2:
            ## 39. Bakun -> Kibungan, Benguet (Device ID: 1381)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1381'
            print "downloading Bakun2"
            urllib.urlretrieve(url, "ASTI//bakw.csv")
            
        else:
            ## 39. Bakun -> Buguias, Benguet (Device ID: 454)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=454'
            print "downloading Bakun3"
            urllib.urlretrieve(url, "ASTI//bakw.csv")

    if site == 'banw':
        if gauge_num == 1:
            ## 39. Banlasan -> Bisu-Calape Campus, Calape, Bohol (Device ID: 735)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=735'
            print "downloading Banlasan"
            urllib.urlretrieve(url, "ASTI//banw.csv")
            
        elif gauge_num == 2:
            ## 39. Banlasan -> Bisu, Clarin, Bohol (Device ID: 107)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=107'
            print "downloading Banlasan2"
            urllib.urlretrieve(url, "ASTI//banw.csv")
            
        else:
            ## 39. Banlasan -> Cambagui Motorpool, Sevilla, Bohol (Device ID: 1559)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1559'
            print "downloading Banlasan3"
            urllib.urlretrieve(url, "ASTI//banw.csv")            

    if site == 'barw':
        if gauge_num == 1:
            ## 36. Baras -> Northwest Samar State University, Calbayog City, Samar (Device ID: 91)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=91'
            print "downloading Baras"
            urllib.urlretrieve(url, "ASTI//barw.csv")
            
        elif gauge_num == 2:
            ## 36. Baras -> Gandara Bridge, Gandara, Samar (Device ID: 550)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=550'
            print "downloading Baras2"
            urllib.urlretrieve(url, "ASTI//barw.csv")
            
        else:
            ## 36. Baras -> Biliran Municipal Ground, Biliran, Biliran (Device ID: 1159)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1159'
            print "downloading Baras3"
            urllib.urlretrieve(url, "ASTI//barw.csv")

    if site == 'batw':
        if gauge_num == 1:
            ## 39. Bato -> PSHS Central Visayas, Argao, Cebu (Device ID: 191)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=191'
            print "downloading Bato"
            urllib.urlretrieve(url, "ASTI//batw.csv")
            
        elif gauge_num == 2:
            ## 39. Bato -> Brgy. Lawaan, Alcantara, Cebu (Device ID: 1647)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1647'
            print "downloading Bato2"
            urllib.urlretrieve(url, "ASTI//batw.csv")
            
        else:
            ## 39. Bato -> Moalboal, Cebu (Device ID: 1474)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1474'
            print "downloading Bato3"
            urllib.urlretrieve(url, "ASTI//batw.csv")

    if site == 'baytcw':
        if gauge_num == 1:
            ## 07. Bayabas -> Villa Hermosa, Lopez, Quezon (Device ID: 1096)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1096'
            print "downloading Bayabas"
            urllib.urlretrieve(url, "ASTI//baytcw.csv")
            
        elif gauge_num == 2:
            ## 07. Bayabas -> San Vicente, Camarines Norte (Device ID: 813)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=813'
            print "downloading Bayabas2"
            urllib.urlretrieve(url, "ASTI//baytcw.csv")
            
        else:
            ## 07. Bayabas -> San Lorenzo Ruiz, Camarines Norte (Device ID: 812)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=812'
            print "downloading Bayabas3"
            urllib.urlretrieve(url, "ASTI//baytcw.csv")
    
    if site == 'blcw' or site == 'blcsaw':
        if gauge_num == 1:
            ## Boloc -> Alimodian, Iloilo (Device ID: 204)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
            print "downloading BLC"
            urllib.urlretrieve(url, "ASTI/blcw.csv")
            urllib.urlretrieve(url, "ASTI/blcsaw.csv")
            
        elif gauge_num == 2:
            ## Boloc -> Maasin, Iloilo (Device ID: 289)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=289'
            print "downloading BLC2"
            urllib.urlretrieve(url, "ASTI/blcw.csv")
            urllib.urlretrieve(url, "ASTI/blcsaw.csv")
        
        else:
            ## Boloc -> San Miguel, Iloilo (Device ID: 258)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=258'
            print "downloading BLC3"
            urllib.urlretrieve(url, "ASTI/blcw.csv")
            urllib.urlretrieve(url, "ASTI/blcsaw.csv")
        
    if site == 'bolw':
        if gauge_num == 1:
            ## Bolodbolod -> Poblacion, San Juan, Southern Leyte (Device ID: 1236)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1236'
            print "downloading BOL"
            urllib.urlretrieve(url, "ASTI/bolw.csv")
            
        elif gauge_num == 2:
            ## Bolodbolod -> Municipal Plaza, Libagon, Southern Leyte (Device ID: 1237)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1237'
            print "downloading BOL2"
            urllib.urlretrieve(url, "ASTI/bolw.csv")
            
        else:
            ## Bolodbolod -> Hinunangan Campus, Hinunangan, Southern Leyte (Device ID: 538)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=538'
            print "downloading BOL3"
            urllib.urlretrieve(url, "ASTI/bolw.csv")

    if site == 'cartaw':
        if gauge_num == 1:
            ## 18. SanCarlos -> Municipal Hall, Claver, Surigao del Norte (Device ID: 154)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=154'
            print "downloading SanCarlos"
            urllib.urlretrieve(url, "ASTI//cartaw.csv")
            
        elif gauge_num == 2:
            ## 18. SanCarlos -> Gigaquit, Surigao del Norte (Device ID: 1204)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1204'
            print "downloading SanCarlos2"
            urllib.urlretrieve(url, "ASTI//cartaw.csv")
            
        else:
            ## 18. SanCarlos -> Poblacion, Gigaquit, Surigao del Norte (Device ID: 1387)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1387'
            print "downloading SanCarlos3"
            urllib.urlretrieve(url, "ASTI//cartaw.csv")
        
    if site == 'cudtaw':
        if gauge_num == 1:
            ## Cudog -> Ibulao Bridge, Lagfawe, Ifugao (Device ID: 464)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=464'
            print "downloading CUD"
            urllib.urlretrieve(url, "ASTI/cudtaw.csv")
            
        elif gauge_num == 2:
            ## Cudog -> Lamut, Ifugao (Device ID: 1104)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1104'
            print "downloading CUD2"
            urllib.urlretrieve(url, "ASTI/cudtaw.csv")
            
        else:
            ## Cudog -> Municipal Hall, Kiangan, Ifugao (Device ID: 470)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=470'
            print "downloading CUD3"
            urllib.urlretrieve(url, "ASTI/cudtaw.csv")
            
    if site == 'dadtbw':
        if gauge_num == 1:
            ## 04. Dadong -> Tarragona, Davao Oriental (Device ID: 733)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=733'
            print "downloading Dadong"
            urllib.urlretrieve(url, "ASTI//dadtbw.csv")
            
        elif gauge_num == 2:
            ## 04. Dadong -> Manay, Davao Oriental (Device ID: 732)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=732'
            print "downloading Dadong2"
            urllib.urlretrieve(url, "ASTI//dadtbw.csv")
            
        else:
            ## 04. Dadong -> Caraga, Davao Oriental (Device ID: 729)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=729'
            print "downloading Dadong3"
            urllib.urlretrieve(url, "ASTI//dadtbw.csv")
        
    if site == 'gaaw':
        if gauge_num == 1:
            ## 33. Gaas -> Municipal Hall, Balamban, Cebu (Device ID: 1219)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1219'
            print "downloading Gaas"
            urllib.urlretrieve(url, "ASTI//gaaw.csv")
            
        elif gauge_num == 2:
            ## 33. Gaas -> Talamban Campus, Talamban, Cebu (Device ID: 1578)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1578'
            print "downloading Gaas2"
            urllib.urlretrieve(url, "ASTI//gaaw.csv")
            
        else:
            ## 33. Gaas -> Brgy. Panas, Consolacion, Cebu (Device ID: 1499)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1499'
            print "downloading Gaas3"
            urllib.urlretrieve(url, "ASTI//gaaw.csv")
                   
    if site == 'gamw':
        if gauge_num == 1:
            ## Gamut -> Awasian, Tandag City, Surigao del Sur (Device ID: 782)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=782'
            print "downloading GAM"
            urllib.urlretrieve(url, "ASTI/gamw.csv")
            
        elif gauge_num == 2:
            ## Bolodbolod -> Tago Poblacion, Tago, Surigao del Sur (Device ID: 1574)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1574'
            print "downloading GAM2"
            urllib.urlretrieve(url, "ASTI/bolw.csv")
            
        else:
            ## Bolodbolod -> Balite, Bayabas, Surigao del Sur (Device ID: 781)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=781'
            print "downloading GAM3"
            urllib.urlretrieve(url, "ASTI/bolw.csv")
        
    if site == 'hinw':
        if gauge_num == 1:
        	## 32. Hinabangan -> Municipal Hall, Paranas, Samar (Device ID: 619)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=619'
            print "downloading Hinabangan"
            urllib.urlretrieve(url, "ASTI//hinw.csv")
            
        elif gauge_num == 2:
        	## 32. Hinabangan -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
            print "downloading Hinabangan2"
            urllib.urlretrieve(url, "ASTI//hinw.csv")
            
        else:
        	## 32. Hinabangan -> Poblacion, Babatgon, Leyte (Device ID: 546)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=546'
            print "downloading Hinabangan3"
            urllib.urlretrieve(url, "ASTI//hinw.csv")
            
    if site == 'humw':
        if gauge_num == 1:
            ## Humayhumay -> PSTC Negros Oriental, Dumaguete City, Negros Oriental (Device ID: 108)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=108'
            print "downloading HUM"
            urllib.urlretrieve(url, "ASTI/humw.csv")        
        
        elif gauge_num == 2:
            ## Humayhumay -> Canlaon, Canlaon City, Negros Oriental (Device ID: 789)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=789'
            print "downloading HUM2"
            urllib.urlretrieve(url, "ASTI/humw.csv")
            
        else:
            ## Humayhumay -> Poblacion, Jimalalud, Negros Oriental (Device ID: 1492)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1492'
            print "downloading HUM3"
            urllib.urlretrieve(url, "ASTI/humw.csv")
        
    if site == 'imew':
        if gauge_num == 1:
            ## 35. Imelda -> Fandara Bridge, Gandara, Samar (Device ID: 550)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=550'
            print "downloading Imelda"
            urllib.urlretrieve(url, "ASTI//imew.csv")
            
        elif gauge_num == 2:
            ## 35. Imelda -> Northwest Samar State University, Calbayog City, Samar (Device ID: 91)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=91'
            print "downloading Imelda2"
            urllib.urlretrieve(url, "ASTI//imew.csv")
            
        else:
            ## 35. Imelda -> Biliran Municipal Ground, Biliran, Biliran (Device ID: 1159)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1159'
            print "downloading Imelda3"
            urllib.urlretrieve(url, "ASTI//imew.csv")

    if site == 'imuw':
        if gauge_num == 1:
            ## 39. Immuli -> Pidigan, Abra (Device ID: 1188)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1188'
            print "downloading Immuli"
            urllib.urlretrieve(url, "ASTI//imuw.csv")
            
        elif gauge_num == 2:
            ## 39. Immuli -> San Isidro, Abra (Device ID: 1305)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1305'
            print "downloading Immuli2"
            urllib.urlretrieve(url, "ASTI//imuw.csv")
            
        else:
            ## 39. Immuli -> Penarubia, Abra (Device ID: 1283)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1283'
            print "downloading Immuli3"
            urllib.urlretrieve(url, "ASTI//imuw.csv")

    if site == 'inaw':
        if gauge_num == 1:
            ## 20. Inabasan -> Maasin, Iloilo (Device ID: 289)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=289'
            print "downloading Inabasan"
            urllib.urlretrieve(url, "ASTI//inaw.csv")
            
        elif gauge_num == 2:
            ## 20. Inabasan -> Cabatuan, Iloilo (Device ID: 203)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=203'
            print "downloading Inabasan2"
            urllib.urlretrieve(url, "ASTI//inaw.csv")
            
        else:
            ## 20. Inabasan -> Alimodian, Iloilo (Device ID: 204)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
            print "downloading Inabasan3"
            urllib.urlretrieve(url, "ASTI//inaw.csv")
        
    if site == 'kanw':
        if gauge_num == 1:
            ## 15. Kanaan -> MDRRM OFFICE, IGACOS, DAVAO DEL NORTE (Device ID: 1459)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1459'
            print "downloading Kanaan"
            urllib.urlretrieve(url, "ASTI//kanw.csv")
            
        elif gauge_num == 2:
            ## 15. Kanaan -> Matina Pangi, Davao City (Device ID: 954)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=954'
            print "downloading Kanaan2"
            urllib.urlretrieve(url, "ASTI//kanw.csv")
            
        else:
            ## 15. Kanaan -> Waan Bridge, Mandug, Davao City (Device ID: 1177)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1177'
            print "downloading Kanaan3"
            urllib.urlretrieve(url, "ASTI//kanw.csv")
        
    if site == 'labw':
        if gauge_num == 1:
            ## Labey -> Tocmo, Benguet, Benguet (Device ID: 1383)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1383'
            print "downloading LAB"
            urllib.urlretrieve(url, "ASTI/labw.csv")
            
        elif gauge_num == 2:
            ## Labey -> Atok, Benguet (Device ID: 1379)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1379'
            print "downloading LAB2"
            urllib.urlretrieve(url, "ASTI/labw.csv")
            
        else:
            ## Labey -> Itogon, Benguet (Device ID: 478)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=478'
            print "downloading LAB3"
            urllib.urlretrieve(url, "ASTI/labw.csv")
        
    if site == 'layw':
        if gauge_num == 1:
        	## 29. Laygayon -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
            print "downloading Laygayon"
            urllib.urlretrieve(url, "ASTI//layw.csv")
            
        elif gauge_num == 2:
        	## 29. Laygayon -> Poblacion, Babatgon, Leyte (Device ID: 546)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=546'
            print "downloading Laygayon2"
            urllib.urlretrieve(url, "ASTI//layw.csv")
            
        else:
        	## 29. Laygayon -> Municipal Hall, Paranas, Samar (Device ID: 619)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=619'
            print "downloading Laygayon3"
            urllib.urlretrieve(url, "ASTI//layw.csv")
    	
    if site == 'lipw':
        if gauge_num == 1:
            ## Lipanto -> Poblacion, San Juan, Southern Leyte (Device ID: 1236)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1236'
            print "downloading LIP"
            urllib.urlretrieve(url, "ASTI/lipw.csv")
        
        elif gauge_num == 2:
            ## Lipanto -> Municipal Plaza, Libagon, Southern Leyte (Device ID: 1237)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1237'
            print "downloading LIP2"
            urllib.urlretrieve(url, "ASTI/lipw.csv")
            
        else:
            ## Lipanto -> Poblacion, Anahawan, Southern Leyte (Device ID: 1235)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1235'
            print "downloading LIP3"
            urllib.urlretrieve(url, "ASTI/lipw.csv")
            
    if site == 'lptw':
        if gauge_num == 1:
        	## 27. Lipata -> Paranas Municipal Hall, Samar (Device ID: 619)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=619'
            print "downloading Lipata"
            urllib.urlretrieve(url, "ASTI//lptw.csv")
            
        elif gauge_num == 2:
        	## 27. Lipata -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
            print "downloading Lipata2"
            urllib.urlretrieve(url, "ASTI//lptw.csv")
            
        else:
        	## 27. Lipata -> Gandara Bridge, Gandara, Samar (Device ID: 550)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=550'
            print "downloading Lipata3"
            urllib.urlretrieve(url, "ASTI//lptw.csv")

    if site == 'lunw':
        if gauge_num == 1:
            ## 08. Lunas -> PSTC Southern Leyte, Maasin, Southern Leyte (Device ID: 89)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=89'
            print "downloading Lunas"
            urllib.urlretrieve(url, "ASTI//lunw.csv")
            
        elif gauge_num == 2:
            ## 08. Lunas -> Bontoc, Southern Leyte (Device ID: 1238)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1238'
            print "downloading Lunas2"
            urllib.urlretrieve(url, "ASTI//lunw.csv")
            
        else:
            ## 08. Lunas -> Municipal Hall, Libagon, Southern Leyte (Device ID: 1237)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1237'
            print "downloading Lunas3"
            urllib.urlretrieve(url, "ASTI//lunw.csv")
            
    if site == 'magw':
        if gauge_num == 1:
            ## 11. Magsaysay -> Palacapao, Quezon, Bukidnon (Device ID: 505)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=505'
            print "downloading Magsaysay"
            urllib.urlretrieve(url, "ASTI//magw.csv")
            
        elif gauge_num == 2:
            ## 11. Magsaysay -> Arakan, Cotobato (Device ID: 1109)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1109'
            print "downloading Magsaysay2"
            urllib.urlretrieve(url, "ASTI//magw.csv")
            
        else:
            ## 11. Magsaysay -> Municipal Hall, Quezon, Bukidnon (Device ID: 499)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=499'
            print "downloading Magsaysay3"
            urllib.urlretrieve(url, "ASTI//magw.csv")
        
    if site == 'mamw':
        if gauge_num == 1:
            ## Mamuyod -> Tocmo, Benguet, Benguet (Device ID: 1383)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1383'
            print "downloading MAM"
            urllib.urlretrieve(url, "ASTI/mamw.csv")
            
        elif gauge_num == 2:
            ## Mamuyod -> Atok, Benguet (Device ID: 1379)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1379'
            print "downloading MAM2"
            urllib.urlretrieve(url, "ASTI/mamw.csv")
        
        else:
            ## Mamuyod -> Balili, La Trinidad, Benguet (Device ID: 1390)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1390'
            print "downloading MAM3"
            urllib.urlretrieve(url, "ASTI/mamw.csv")        
        
    if site == 'manw':
        if gauge_num == 1:
        	## 31. Manghulyawon -> Poblacion, Jimalud, Negros Oriental (Device ID: 1492)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1492'
            print "downloading Manghulyawon"
            urllib.urlretrieve(url, "ASTI//manw.csv")      
            
        elif gauge_num == 2:
        	## 31. Manghulyawon -> PSTC, Dumaguete City, Negros Oriental (Device ID: 108)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=108'
            print "downloading Manghulyawon2"
            urllib.urlretrieve(url, "ASTI//manw.csv")
        
        else:
        	## 31. Manghulyawon -> South Poblacion, Ayungon, Negros Oriental (Device ID: 681)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=681'
            print "downloading Manghulyawon3"
            urllib.urlretrieve(url, "ASTI//manw.csv")		

    if site == 'marw':
        if gauge_num == 1:
            ## 23. Marirong -> Alimodian, Iloilo (Device ID: 204)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
            print "downloading Marirong"
            urllib.urlretrieve(url, "ASTI//marw.csv")
            
        elif gauge_num == 2:
            ## 23. Marirong -> Maasin, Iloilo (Device ID: 289)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=289'
            print "downloading Marirong2"
            urllib.urlretrieve(url, "ASTI//marw.csv")
            
        else:
            ## 23. Marirong -> San Miguel, Iloilo (Device ID: 258)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=258'
            print "downloading Marirong3"
            urllib.urlretrieve(url, "ASTI//marw.csv")
        
    if site == 'mcataw':
        if gauge_num == 1:
            ## 12. McArthur -> DON FLAVIA, SAN LUIS, AGUSAN DEL SUR (Device ID: 607)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=607'
            print "downloading McArthur"
            urllib.urlretrieve(url, "ASTI//mcataw.csv")
            
        elif gauge_num == 2:
            ## 12. McArthur -> POBLACION, ESPERANZA, AGUSAN DEL SUR (Device ID: 609)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=609'
            print "downloading McArthur2"
            urllib.urlretrieve(url, "ASTI//mcataw.csv")
            
        else:
            ## 12. McArthur -> SAN VICENTE, PROSPERIDAD, AGUSAN DEL SUR (Device ID: 589)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=589'
            print "downloading McArthur3"
            urllib.urlretrieve(url, "ASTI//mcataw.csv")
               
    if site == 'nagtbw':
        if gauge_num == 1:
            ## Nagyubuyuban -> Mamat-ing Bridge, Naguilian, La Union (Device ID: 1069)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=152'
            print "downloading Nagyubuyuban"
            urllib.urlretrieve(url, "ASTI/nagtbw.csv")
        
        elif gauge_num == 2:
            ## CABAROAN BRIDGE, SAN JUAN  La Union (Device ID: 849)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1573'
            print "downloading Nagyubuyuban2"
            urllib.urlretrieve(url, "ASTI/nagtbw.csv")

        else:
            ## Nagyubuyuban -> SAN FERNANDO AIRPORT, SAN FERNANDO CITY  La Union (Device ID: 96)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1567'
            print "downloading Nagyubuyuban3"
            urllib.urlretrieve(url, "ASTI/nagtbw.csv")
        
    if site == 'ninw':
        if gauge_num == 1:
            ## 16. Sto. Nino -> Talaingod, Davao del Norte (Device ID: 858)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=858'
            print "downloading Nino"
            urllib.urlretrieve(url, "ASTI//ninw.csv")
            
        elif gauge_num == 2:
            ## 16. Sto. Nino -> Semonmg Bridge, Kapalong, Davao del Norte (Device ID: 1554)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1554'
            print "downloading Nino2"
            urllib.urlretrieve(url, "ASTI//ninw.csv")
            
        else:
            ## 16. Sto. Nino -> Municipal Hall, Talaingod, Davao del Norte (Device ID: 1457)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1457'
            print "downloading Nino3"
            urllib.urlretrieve(url, "ASTI//ninw.csv")
       
    if site == 'nurw':
        if gauge_num == 1:
            ## 19. Nurcia -> Municipal Hall, Carmen, Surigao del Sur (Device ID: 1561)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1561'
            print "downloading Nurcia"
            urllib.urlretrieve(url, "ASTI//nurtbw.csv")
            
        elif gauge_num == 2:
            ## 19. Nurcia -> Florita Herrera-Irizari NHS, Surigao del Sur (Device ID: 1576)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1576'
            print "downloading Nurcia2"
            urllib.urlretrieve(url, "ASTI//nurtbw.csv")
            
        else:
            ## 19. Nurcia -> Carmen, Surigao del Sur (Device ID: 1577)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1577'
            print "downloading Nurcia3"
            urllib.urlretrieve(url, "ASTI//nurtbw.csv")

    if site == 'oslw':
        if gauge_num == 1:
            ## Oslao -> Municipal Hall, San Francisco, Surigao del Norte (Device ID: 152)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=152'
            print "downloading OSL"
            urllib.urlretrieve(url, "ASTI/oslw.csv")
        
        elif gauge_num == 2:
            ## Oslao -> Mat-I NHS, Surigao City, Surigao del Sur (Device ID: 1573)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1573'
            print "downloading OSL2"
            urllib.urlretrieve(url, "ASTI/oslw.csv")

        else:
            ## Oslao -> Bonifacio ES, Surigao City, Surigao del Norte (Device ID: 1567)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1567'
            print "downloading OSL3"
            urllib.urlretrieve(url, "ASTI/oslw.csv")
        
    if site == 'panw':
        if gauge_num == 1:
            ## 38. Pange -> Bus Terminal, Allen, Northern Samar (Device ID: 1535)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1535'
            print "downloading Pange"
            urllib.urlretrieve(url, "ASTI//panw.csv")
            
        elif gauge_num == 2:
            ## 38. Pange -> Municipal Grounds, Lavezares, Northern Samar (Device ID: 1536)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1536'
            print "downloading Pange2"
            urllib.urlretrieve(url, "ASTI//panw.csv")
            
        else:
            ## 38. Pange -> Municipal Building, San Antonio, Northern Samar (Device ID: 1616)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1616'
            print "downloading Pange3"
            urllib.urlretrieve(url, "ASTI//panw.csv")
            
    if site == 'parw':
        if gauge_num == 1:
        	## 30. Parasanon -> Brgy. Dolores, Pinabacdao, Samar (Device ID: 535)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=535'
            print "downloading Parasanon"
            urllib.urlretrieve(url, "ASTI//parw.csv")
            
        elif gauge_num == 2:
        	## 30. Parasanon -> Poblacion, Babatgon, Leyte (Device ID: 546)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=546'
            print "downloading Parasanon2"
            urllib.urlretrieve(url, "ASTI//parw.csv")
            
        else:
        	## 30. Parasanon -> Municipal Hall, Paranas, Samar (Device ID: 619)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=619'
            print "downloading Parasanon3"
            urllib.urlretrieve(url, "ASTI//parw.csv")
    	
    if site == 'pepsbw':
        if gauge_num == 1:
            ## 22. Pepe -> Alimodian, Iloilo (Device ID: 204)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
            print "downloading Pepe"
            urllib.urlretrieve(url, "ASTI//pepw.csv")
            
        elif gauge_num == 2:
            ## 22. Pepe -> San Miguel, Iloilo (Device ID: 258)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=258'
            print "downloading Pepe2"
            urllib.urlretrieve(url, "ASTI//pepw.csv")
        
        else:
            ## 22. Pepe -> Cabatuan, Iloilo (Device ID: 203)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=203'
            print "downloading Pepe3"
            urllib.urlretrieve(url, "ASTI//pepw.csv")

    if site == 'pinw':
        if gauge_num == 1:
            ## 24. Pinagkamaligan -> Villahermosa, Lopez, Quezon (Device ID: 1096)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1096'
            print "downloading Pinagkamaligan"
            urllib.urlretrieve(url, "ASTI//pinw.csv")
            
        elif gauge_num == 2:
            ## 24. Pinagkamaligan -> Municipal Hall, Unisan, Quezon (Device ID: 198)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=198'
            print "downloading Pinagkamaligan2"
            urllib.urlretrieve(url, "ASTI//pinw.csv")
            
        else:
            ## 24. Pinagkamaligan -> Buenavista, Mulanay, Quezon (Device ID: 1160)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1160'
            print "downloading Pinagkamaligan3"
            urllib.urlretrieve(url, "ASTI//pinw.csv")

    if site == 'plaw':
        if gauge_num == 1:
            ## Planas -> PSTC Negros Oriental, Dumaguete City, Negros Oriental (Device ID: 108)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=108'
            print "downloading PLA"
            urllib.urlretrieve(url, "ASTI/plaw.csv")
            
        elif gauge_num == 2:
            ## Planas -> Poblacion, Jimalalud, Negros Oriental (Device ID: 1492)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1492'
            print "downloading PLA2"
            urllib.urlretrieve(url, "ASTI/plaw.csv")
            
        else:
            ## Planas -> Canlaon, Canlaon City, Negros Oriental (Device ID: 789)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=789'
            print "downloading PLA3"
            urllib.urlretrieve(url, "ASTI/plaw.csv")
        
    if site == 'pobw':
        if gauge_num == 1:
        	## 26. Poblacion I -> Gandara Bridge, Gandara, Samar (Device ID: 550)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=550'
            print "downloading Poblacion"
            urllib.urlretrieve(url, "ASTI//pobw.csv")
            
        elif gauge_num == 2:
        	## 26. Poblacion I -> Northwest Samar State U, Calbayog City, Samar (Device ID: 91)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=91'
            print "downloading Poblacion2"
            urllib.urlretrieve(url, "ASTI//pobw.csv")
            
        else:
        	## 26. Poblacion I -> Municipal Hall, Paranas, Samar (Device ID: 619)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=619'
            print "downloading Poblacion3"
            urllib.urlretrieve(url, "ASTI//pobw.csv")
    
    if site == 'pugw':
        if gauge_num == 1:
            ## Puguis -> DOST Regional Office, La Trinidad, Benguet (Device ID: 65)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=65'
            print "downloading PUG"
            urllib.urlretrieve(url, "ASTI/pugw.csv")
            
        elif gauge_num == 2:
            ## Puguis -> Balili, La Trinidad, Benguet (Device ID: 1390)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1390'
            print "downloading PUG2"
            urllib.urlretrieve(url, "ASTI/pugw.csv")
            
        else:
            ## Puguis -> Irisan, Benguet (Device ID: 69)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=69'
            print "downloading PUG3"
            urllib.urlretrieve(url, "ASTI/pugw.csv")
        
    if site == 'sagtaw':
        if gauge_num == 1:
            ## Antadao -> Sagada, Mountain Province (Device ID: 467)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=467'
            print "downloading SAG"
            urllib.urlretrieve(url, "ASTI/sagtaw.csv")
            
        elif gauge_num == 2:
            ## Antadao -> Besao, Mountain Province (Device ID: 449)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=449'
            print "downloading SAG2"
            urllib.urlretrieve(url, "ASTI/sagtaw.csv")
            
        else:
            ## Antadao -> Sabangan, Mountain Province (Device ID: 1148)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1148'
            print "downloading SAG3"
            urllib.urlretrieve(url, "ASTI/sagtaw.csv")

    if site == 'sibtaw':
        if gauge_num == 1:
            ## 05. Sibahay -> Cabasagan, Boston, Davao Oriental (Device ID: 1450)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1450'
            print "downloading Sibahay"
            urllib.urlretrieve(url, "ASTI//sibtaw.csv")
            
        elif gauge_num == 2:
            ## 05. Sibahay -> Boston, Davao Oriental (Device ID: 728)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=728'
            print "downloading Sibahay2"
            urllib.urlretrieve(url, "ASTI//sibtaw.csv")
            
        else:
            ## 05. Sibahay -> Cateel, Davao Oriental (Device ID: 730)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=730'
            print "downloading Sibahay3"
            urllib.urlretrieve(url, "ASTI//sibtaw.csv")
                        
    if site == 'sinw':
        if gauge_num == 1:
            ## Sinipsip -> Buguias, Benguet (Device ID: 454)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=454'
            print "downloading SIN"
            urllib.urlretrieve(url, "ASTI/sinw.csv")
            
        elif gauge_num == 2:
            ## Sinipsip -> Bakun, Benguet (Device ID: 450)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=450'
            print "downloading SIN2"
            urllib.urlretrieve(url, "ASTI/sinw.csv")
            
        else:
            ## Sinipsip -> Kabayan, Benguet (Device ID: 1150)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1150'
            print "downloading SIN3"
            urllib.urlretrieve(url, "ASTI/sinw.csv")
        
    if site == 'sumw':
        if gauge_num == 1:
            ## 10. Sumalsag -> ARCH BRIDGE, MALITBOG, BUKIDNON (Device ID: 760)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=760'
            print "downloading Sumalsag"
            urllib.urlretrieve(url, "ASTI//sumtbw.csv")
            
        elif gauge_num == 2:
            ## 10. Sumalsag -> SAN LUIS, MALITBOG, BUKIDNON (Device ID: 487)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=487'
            print "downloading Sumalsag2"
            urllib.urlretrieve(url, "ASTI//sumtbw.csv")
        
        else:
            ## 10. Sumalsag -> TAGOLOAN, BUKIDNON (Device ID: 489)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=489'
            print "downloading Sumalsag3"
            urllib.urlretrieve(url, "ASTI//sumtbw.csv")        
        
    if site == 'tagw':
        if gauge_num == 1:
            ## 40. Taga -> Pinukpuk, Kalinga (Device ID: 1380)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1380'
            print "downloading Taga"
            urllib.urlretrieve(url, "ASTI//tagw.csv")
            
        elif gauge_num == 2:
            ## 40. Taga -> Ninoy Aquino Bridge, Tuao, Cagayan (Device ID: 1089)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1089'
            print "downloading Taga2"
            urllib.urlretrieve(url, "ASTI//tagw.csv")
            
        else:
            ## 40. Ifugao State Univeristy, Lamut, Ifugao (Device ID: 66)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=66'
            print "downloading Taga3"
            urllib.urlretrieve(url, "ASTI//tagw.csv")

    if site == 'talw':
        if gauge_num == 1:
            ## 37. Talahid -> Almeria Central School, Almeria, Biliran(Device ID: 1158)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1158'
            print "downloading Talahid"
            urllib.urlretrieve(url, "ASTI//talw.csv")
            
        elif gauge_num == 2:
            ## 37. Talahid -> Kawayan, Biliran (Device ID: 516)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=516'
            print "downloading Talahid2"
            urllib.urlretrieve(url, "ASTI//talw.csv")
            
        else:
            ## 37. Talahid -> Brgy. Calumpang, Naval, Biliran (Device ID: 90)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=90'
            print "downloading Talahid3"
            urllib.urlretrieve(url, "ASTI//talw.csv")

    if site == 'tamw':
        if gauge_num == 1:
            ## 39. Tamac -> Brgy. Cabaroan, San Emilio, Ilocos Sur (Device ID: 645)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=645'
            print "downloading Tamac"
            urllib.urlretrieve(url, "ASTI//tamw.csv")
            
        elif gauge_num == 2:
            ## 39. Tamac -> Luba, Abra (Device ID: 1302)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1302'
            print "downloading Tamac2"
            urllib.urlretrieve(url, "ASTI//tamw.csv")
            
        else:
            ## 39. Tamac -> Pilar, Abra (Device ID: 466)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=466'
            print "downloading Tamac3"
            urllib.urlretrieve(url, "ASTI//tamw.csv")
            
    if site == 'tuetbw':
        if gauge_num == 1:
            ## Tue -> Tadian, Mountain Province (Device ID: 469)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=469'
            print "downloading TUE"
            urllib.urlretrieve(url, "ASTI/tuetbw.csv")
            
        elif gauge_num == 2:
            ## Tue -> Sabangan, Mountain Province (Device ID: 1148)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1148'
            print "downloading TUE2"
            urllib.urlretrieve(url, "ASTI/tuetbw.csv")
            
        else:
            ## Tue -> Aluling Bridge, Tadian, Mountain Province (Device ID: 1425)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1425'
            print "downloading TUE3"
            urllib.urlretrieve(url, "ASTI/tuetbw.csv")
                
    if site == 'umiw':
        if gauge_num == 1:
            ## 21. Umingan -> Alimodian, Iloilo (Device ID: 204)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
            print "downloading Umingan"
            urllib.urlretrieve(url, "ASTI//umiw.csv")
            
        elif gauge_num == 2:
            ## 21. Umingan -> San Miguel, Iloilo (Device ID: 258)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=258'
            print "downloading Umingan2"
            urllib.urlretrieve(url, "ASTI//umiw.csv")
            
        else:
            ## 21. Umingan -> Cabatuan, Iloilo (Device ID: 203)
            url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=203'
            print "downloading Umingan3"
            urllib.urlretrieve(url, "ASTI//umiw.csv")