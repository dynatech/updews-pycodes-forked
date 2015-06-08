import urllib
import urllib2
## This will download CSV files that contain 14 days worth of data

## BLC
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
print "downloading Boloc"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\blc.csv")

## BOL
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1236'
print "downloading Bolod-Bolod"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\bol.csv")

## GAM
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=782'
print "downloading Gamut"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\gam.csv")

## HUM
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=789'
print "downloading Humay"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\hum.csv")

## LAB
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=389'
print "downloading Labey"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\lab.csv")

## LIP
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1236'
print "downloading Lipanto"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\lip.csv")

## MAM
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=389'
print "downloading Mamuyod"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\mam.csv")

## OSL
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=152'
print "downloading Oslao"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\osl.csv")

## PLA
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=789'
print "downloading Planas"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\pla.csv")

## PUG
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=65'
print "downloading Puguis"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\pug.csv")

## SIN
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=454'
print "downloading Sinipsip"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\sin.csv")

## SAG
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=467'
print "downloading Sagada"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\sag.csv")

## TUE
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=469'
print "downloading Tue"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\tue.csv")

## CUD
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=391'
print "downloading Cudog"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\cud.csv")

## 02. Baretto -> Abucay (Device ID: 1103)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1103'
print "downloading Baretto"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\bar.csv")

## 04. Dadong -> Tarragona (Device ID: 733)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=733'
print "downloading Dadong"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\dad.csv")

## 05. Sibahay -> Brgy. Cabasagan, Boston (Device ID: 1450)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1450'
print "downloading Sibahay"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\sib.csv")

## 06. Agbatuan-> Brgy. Rapulang, Maayon (Device ID: 557)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=557'
print "downloading Agbatuan"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\agb.csv")

## 07. Bayabas -> CNSC, Jose Panganiban (Device ID: 79)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=79'
print "downloading Bayabas"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\bay.csv")

## 08. Lunas -> PSTC SOUTHERN LEYTE, MAASIN (Device ID: 89)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=89'
print "downloading Lunas"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\lun.csv")

## 10. Sumalsag -> ARCH BRIDGE, MALITBOG (Device ID: 760)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=760'
print "downloading Sumalsag"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\sum.csv")

## 11. Magsaysay -> Dangcagan (Device ID: 867)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=867'
print "downloading Magsaysay"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\mag.csv")

## 12. McArthur -> DON FLAVIA, SAN LUIS (Device ID: 607
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=607'
print "downloading McArthur"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\mac.csv")

## 14. Pitu -> SULOP, POBLACION, SULOP (Device ID: 363)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=363'
print "downloading Pitu"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\pit.csv")

## 15. Kanaan -> MDRRM OFFICE, IGACOS (Device ID: 1459)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1459'
print "downloading Kanaan"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\kan.csv")

## 16. Sto. Nino -> Talaingod, Davao del Norte (Device ID: 858)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=858'
print "downloading Nino"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\nin.csv")

## 17. Monte Duali -> Laak, Davao del Norte (Device ID: 1289)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1289'
print "downloading Monte"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\mon.csv")

## 18. SanCarlos -> Siargao Island, Surigao del Norte (Device ID: 180)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=180'
print "downloading SanCarlos"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\car.csv")

## 19. Nurcia -> Carmen, Surigao del Sur (Device ID: 1561)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1561'
print "downloading Nurcia"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\nur.csv")

## 20. Inabasan -> Maasin, Iloilo (Device ID: 289)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=289'
print "downloading Inabasan"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\ina.csv")

## 21. Umingan -> Alimodian, Iloilo (Device ID: 204)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
print "downloading Umingan"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\umi.csv")

## 22. Pepe -> Alimodian, Iloilo (Device ID: 204)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
print "downloading Pepe"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\pep.csv")

## 23. Marirong -> Alimodian, Iloilo (Device ID: 204)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=204'
print "downloading Marirong"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\mar.csv")

## 24. Pinagkamaligan -> Brgy. Villahermosa, Quezon (Device ID: 1096)
url = 'http://www.dewslandslide.com/ajax/dlRain2.php?site=1096'
print "downloading Pinagkamaligan"
urllib.urlretrieve(url, "C:\DB Mount\Dropbox\Senslope Data\Proc2\Temp\\pin.csv")




