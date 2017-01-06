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

