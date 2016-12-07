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


#DIR=`pwd`
#DIR="$(dirname $DIR)"
#MAINDIR="$(dirname $DIR)"
MAINDIR="/home/vcap/jupyter/sparktk_test"

echo MAINDIR : $MAINDIR

#pushd $MAINDIR
unzip $MAINDIR/sparktk-core*.zip -d $MAINDIR
cd $MAINDIR
sparktkcorepkg=$(find -name "sparktk-core*" -type d)
echo "SPARKTK CORE PACKAGE: $sparktkcorepkg"

mv $sparktkcorepkg $MAINDIR/sparktk-core


#pushd $MAINDIR/sparktk-core
cd $MAINDIR/sparktk-core
echo "CHANGED WORKING DIR TO `pwd`"
echo "RUNNING INSTALL.SH"


unset SPARK_HOME
/bin/bash $MAINDIR/sparktk-core/install.sh

#install python dependencies
echo "INSTALLING PYTHON DYPENDENCIES"
pip2.7 install $MAINDIR/sparktk-core/python/sparktk-*.tar.gz
#popd
#popd
#create a symlink to regression-test/sparkregtest
#pushd "/opt/anaconda2/lib/python2.7/site-packages"
#ls -sfn /home/vcap/jupyter/sparktk_test/regression-tests/sparktkregtests/ . > ignore
#popd
