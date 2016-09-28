#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

# This needs to be kept in sync with the parent POM file
GRAPHFRAMES_SOURCE=http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.1.0-spark1.6/graphframes-0.1.0-spark1.6.jar

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"

echo "Install dependencies"
sudo pip2.7 install --upgrade teamcity-messages pandas numpy scipy statsmodels

echo "Uninstalling spark_tk"
sudo pip2.7 uninstall -y sparktk

echo "installing spark_tk"
sudo pip2.7 install $MAINDIR/*.gz

# Do this before we download the graphframes
echo "inflating jars"
pushd $MAINDIR/regression-tests/automation
cp $MAINDIR/*.zip .
unzip *.zip
popd

echo "Downloading graphframes"
rm -f graphframes.zip
wget -nv --no-check-certificate $GRAPHFRAMES_SOURCE  -O graphframes.zip
unzip -q graphframes.zip

