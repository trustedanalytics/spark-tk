#!/bin/bash
#
#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"

sparktkpackage=$MAINDIR/sparktkinstall

echo "Install dependencies"
sudo pip2.7 install --upgrade -r $DIR/requirements.txt


echo "Uninstalling spark_tk"
sudo pip2.7 uninstall -y sparktk


echo "installing spark_tk"
pushd $MAINDIR
echo FIND PACKAGE
sparkcorepackage=$(find `pwd` -name "sparktk-core*.zip")
corepackage=$(echo $sparkcorepackage | sed -e "s|.zip||g")


echo UNZIP
unzip -o -q $sparkcorepackage

echo RUN_INSTALLER
pushd $corepackage
INSTALLER=$(find `pwd` -name "install.sh")
sudo -E $INSTALLER
graphframes=$(find `pwd` -name "graphframes")
echo "graphframes_package $graphframes"
ls $graphframes/
cp -rv $graphframes $MAINDIR/
popd

mv $corepackage $sparktkpackage

ps -ef | grep scor
for pid in $(ps -ef | grep "model-scor" | awk '{print $2}'); do kill -9 $pid; done

echo "installing scoring engine"
rm -rf $MAINDIR/scoring
mkdir $MAINDIR/scoring
pushd $MAINDIR
scoring_engine=$(find `pwd` -name "model-scoring-java*.zip")
echo $scoring_engine
pushd scoring
rm -rf scoring_engine
mv $scoring_engine ./scoring.zip
unzip -q scoring.zip
rm *.zip
mv model-scoring* scoring_engine
popd
popd
