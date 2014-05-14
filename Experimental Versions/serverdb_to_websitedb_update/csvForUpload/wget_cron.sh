#!/bin/bash

cd /media/sf_csvForUpload

for item in *.csv
do
    echo "Item: $item"
    wget --post-file=`./postencode.py $item` http://senslopebeta2.url.ph/data_upload/upload.php

    if [ $? -ne 0 ]
    then
        echo "wget executed with error"
    else
        echo "wget executed successfully"
	rm $item
    fi
done

echo "clean excess files!"
rm *.post *~ upload* .goutput*


