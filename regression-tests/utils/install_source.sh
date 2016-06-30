#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"

echo "Uninstalling spark_tk"
sudo pip2.7 uninstall sparktk

echo "installing spark_tk"
sudo pip2.7 install $MAINDIR/python/dist/*.gz

echo "linking pyspark"
sudo ln -fs /opt/cloudera/parcels/CDH/lib/spark/python/pyspark /usr/lib/python2.7/site-packages/

echo "inflating jars"
pushd $MAINDIR/core/target
unzip *.zip
popd
