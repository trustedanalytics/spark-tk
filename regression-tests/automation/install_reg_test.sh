#!/bin/bash                                                                                                                              

#path to where the sparktk-regression-tests.tar.gz package is

MAINDIR="/home/vcap/jupyter/sparktk_test"

cd $MAINDIR
echo MAINDIR : $MAINDIR 

#install regression test package
echo "INSTALLING REGRESSION TEST PACKAGE"
tar xzf $MAINDIR/sparktk-regression-tests* -C $MAINDIR
testpkg=$(find -name "sparktk-regression-tests*" -type d)
mv $testpkg $MAINDIR/regression-tests

#create a symlink to regression-test/sparkregtests
cd /opt/anaconda2/lib/python2.7/site-packages
ln -sfn /home/vcap/jupyter/sparktk_test/regression-tests/sparktkregtests/ .

