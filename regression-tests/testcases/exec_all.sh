#!/bin/bash

##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

# This script executes all of the tests located in this folder through the use
# of the nosetests api. Coverage is provided by coverage.py
# This script requires the installation of the install_pyenv.sh script

# '-x' to exclude dirs from coverage, requires nose-exclude to be installed

#export IN_UNIT_TESTS='true'


PYTHON_DIR='/usr/lib/python2.7/site-packages'
TAPROOTANALYTICS_DIR=$PYTHON_DIR/trustedanalytics
TESTS_DIR="$( cd "$( dirname "$BASH_SOURCE[0]}" )" && pwd )"
#RESULTS_DIR='/usr/local/automated_testing/test_runs/results'  # This value is used when running on Automation Clusters
RESULTS_DIR=$TESTS_DIR/results		# Uncomment this line to run on your local cluster	
NUM_PROCS="$((`nproc`/1))" # total number of parallel nose processes
PROC_TIMEOUT="$((3600 * 2))" # let each test runner process to run at most for 2 hours before killing it

echo TESTS_DIR=$TESTS_DIR
echo TAPROOTANALYTICS_DIR=$TAPROOTANALYTICS_DIR
echo PYTHON_DIR=$PYTHON_DIR
echo RESULTS_DIR=$RESULTS_DIR

#Ensure the RESULTS_DIR exists
if [ ! -d "$RESULTS_DIR" ]; then mkdir -p $RESULTS_DIR;fi

#cd $PYTHON_DIR
export PYTHONPATH=$PYTHONPATH:$PYTHON_DIR


if [ "$1" = "-x" ] ; then
  EXCLUDE_DIRS_FILE=$TESTS_DIR/cov_exclude_dirs.txt
  if [[ ! -f $EXCLUDE_DIRS_FILE ]]; then
    echo ERROR: -x option: could not find exclusion file $EXCLUDE_DIRS_FILE
    exit 1
  fi
  echo -x option: excluding files from coverage described in $EXCLUDE_DIRS_FILE
  EXCLUDE_OPTION=--exclude-dir-file=$EXCLUDE_DIRS_FILE
fi

echo "Call to nosetests" >> $RESULTS_DIR/run.log
echo "`date`" >> $RESULTS_DIR/run.log

echo "Kill any existing nosetests in progress"
pkill -f nosetests

time nosetests-2.7 -w $TESTS_DIR -s -vv --with-time --processes=$NUM_PROCS --process-timeout=$PROC_TIMEOUT 1>> $RESULTS_DIR/run.log 2>> $RESULTS_DIR/run.err

success=$?

#COVERAGE_ARCHIVE=$RESULTS_DIR/cover/python-coverage.zip

#rm -rf $COVERAGE_ARCHIVE 
#zip -rq $COVERAGE_ARCHIVE .

RESULT_FILE=$RESULTS_DIR/nosetests.xml
#COVERAGE_HTML=$RESULTS_DIR/cover/index.html

echo 
echo Output File: $RESULT_FILE
#echo Coverage Archive: $COVERAGE_ARCHIVE
#echo Coverage HTML: file://$COVERAGE_HTML
echo 

#unset IN_UNIT_TESTS

if [[ $success == 0 ]] ; then
   echo "Python Tests Successful" >> $RESULTS_DIR/run.log
   echo "`date`" >> $RESULTS_DIR/run.log
   exit 0
fi
echo "Python Tests Unsuccessful" >> $RESULTS_DIR/run.log
echo "`date`" >> $RESULTS_DIR/run.log
exit 1

