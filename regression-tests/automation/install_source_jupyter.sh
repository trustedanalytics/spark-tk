#!/bin/bash

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
