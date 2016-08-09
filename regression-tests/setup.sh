#!/bin/bash
#This file is for installing the library during a test run

sudo yum -y install python27-pip
sudo yum -y install python-devel
#sudo yum -y install cyrus-sasl-devel

sudo pip2.7 install --upgrade requests
sudo pip2.7 list
# The current list of requirements
#sudo pip2.7 install nose nosepipe networkx pyhs2 python-dateutil teamcity-messages - dependencies for nosetest 
#sudo pip2.7 install pytest pytest-xdist networkx pyhs2 python-dateutil teamcity-messages - removing teamcity-messages and pyhs2
sudo pip2.7 install pytest pytest-xdist networkx python-dateutil
sudo pip2.7 install --upgrade pip distribute

rm -rf build
rm -rf dist
#rm -rf ATK_QA_Library.egg-info

# Install datasets
if [ ${ATK_TAP:-0} = 0 ]
then
    utils/install_datasets.sh
else
    utils/install_datasets_remote.sh $HDFS
fi
python2.7 -m compileall .

