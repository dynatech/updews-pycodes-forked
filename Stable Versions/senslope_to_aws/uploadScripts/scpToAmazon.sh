#!/bin/bash

#cd /media/sf_dewslandslide
cd ~/server/websiteUploads
#scp -i /keyPair/senslopeInstance.pem *.sql ubuntu@www.dewslandslide.com:~/sqldumps

#if [ $? -ne 0 ]
#then
#    echo "scp executed with ERROR"
#else
#    echo "scp executed successfully!!!"
#    rm *.sql
#fi

#rm *.post *~ upload* .goutput*

for i in *.sql
do
    scp -i /keyPair/senslopeInstance.pem $i ubuntu@www.dewslandslide.com:~/sqldumps

    if [ "$?" -eq 0 ]; then
        echo "Successfully uploaded $i"
        rm $i
    else
        echo "Failed to upload $i"
    fi
done


