#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"

echo "Install dependencies"
sudo pip2.7 install --upgrade teamcity-mesages pandas numpy scipy

echo "Uninstalling spark_tk"
sudo pip2.7 uninstall -y sparktk

echo "installing spark_tk"
sudo pip2.7 install $MAINDIR/python/dist/*.gz

# Do this before we download the graphframes
echo "inflating jars"
pushd $MAINDIR/regression-tests/automation
ls $MAINDIR/
cp $MAINDIR/*.zip .
ls
unzip *.zip
ls
popd

echo "Downloading graphframes"
rm -f graphframes.zip
wget -nv --no-check-certificate http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.1.0-spark1.5/graphframes-0.1.0-spark1.5.jar -O graphframes.zip
unzip -q graphframes.zip

