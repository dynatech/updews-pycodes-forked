#!/bin/bash

echo 'DEWSL 2.0: Starting installation of Anaconda Requirements'
conda install --file requirements_conda.txt

echo 'DEWSL 2.0: Starting installation of Linux Requirements'
sudo apt -y install libmysqlclient-dev gcc mysql-server memcached

echo 'DEWSL 2.0: Starting installation of Pip Requirements'
pip install --upgrade pip
python -m pip install git+https://github.com/pmarti/python-messaging.git 
pip install serial httplib2 apiclient oauth2client google-api-python-client