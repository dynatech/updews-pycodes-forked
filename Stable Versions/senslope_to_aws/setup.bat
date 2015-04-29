::save the directory path of the setup.bat file
set batchfiledir=%~dp0

::change directory to D: drive
d:

::create a "dewslandslide" and "purged" folder and copy the shell scripts for 
::	automated sending of data to the web server
mkdir dewslandslide
cd dewslandslide
copy "%batchfiledir%uploadScripts\scpToAmazon.sh" "scpToAmazon.sh"

mkdir keyPair
mkdir purged
cd purged
copy "%batchfiledir%uploadScripts\scpToAmazonPurged.sh" "scpToAmazonPurged.sh"

echo "The ssh key is not part of the Git Repository for Security Reasons."
echo "Contact the AWS Senslope Instance creator at updews.prado@gmail.com"
echo "Copy the senslopeInstance.pem in the same directory as this batch file"

pause

copy "%batchfiledir%senslopeInstance.pem" "senslopeInstance.pem"
cd ..\keyPair
copy "%batchfiledir%senslopeInstance.pem" "senslopeInstance.pem"

pause