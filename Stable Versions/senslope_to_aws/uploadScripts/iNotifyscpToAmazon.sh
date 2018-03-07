#!/bin/bash
cd ~/server/websiteUploads

inotifywait -q -m -e close_write ~/server/websiteUploads |
while read folder events filename; do
#    $target = $filename | awk '{ print $(NF) }'
#    if [ "$filename" = *.sql ]; then
    if expr index "$filename" ".sql"; then
        #echo "$filename is a sql file"
        scp -i /keyPair/senslopeInstance.pem $filename ubuntu@www.dewslandslide.com:~/sqldumps

        if [ "$?" -eq 0 ]; then
            echo "Successfully uploaded $filename"
            rm $filename
        else
            echo "Failed to upload $filename"
        fi
    else
        echo "$filename is a file that won't be uploaded"
    fi

#    echo "$filename was written"

#    echo "new event"
done
