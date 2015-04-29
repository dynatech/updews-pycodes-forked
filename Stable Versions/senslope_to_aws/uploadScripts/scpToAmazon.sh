#!/bin/bash

cd /media/sf_dewslandslide
scp -i /keyPair/senslopeInstance.pem *.sql ubuntu@www.dewslandslide.com:~/sqldumps

if [ $? -ne 0 ]
then
    echo "scp executed with ERROR"
else
    echo "scp executed successfully!!!"
    rm *.sql
fi

rm *.post *~ upload* .goutput*

#mysql -u root -psenslope senslopedb < gamb_2014-06-20_153500.sql 





