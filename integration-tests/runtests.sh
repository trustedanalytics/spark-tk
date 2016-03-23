#!/usr/bin/env bash

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
#PYTHON_DIR='/usr/lib/python2.7/site-packages'
#TARGET_DIR=$DIR/target
#OUTPUT1=$TARGET_DIR/surefire-reports/TEST-nose-smoketests.xml
#OUTPUT2=$TARGET_DIR/surefire-reports/TEST-nose-tests.xml
#OUTPUT3=$TARGET_DIR/surefire-reports/TEST-nose-nonconcurrenttests.xml
#export PYTHONPATH=$DIR/../python-client:$PYTHONPATH:$PYTHON_DIR

echo "$NAME DIR=$DIR"
#echo "$NAME PYTHON_DIR=$PYTHON_DIR"
#echo "$NAME PYTHONPATH=$PYTHONPATH"
#echo "$NAME all generated files will go to target dir: $TARGET_DIR"

#echo "$NAME Shutting down old API Server"

export PYSPARK_PYTHON=/usr/bin/python2.7
export PYSPARK_DRIVER_PYTHON=/usr/bin/python2.7
export SPARK_HOME=/home/blbarker/spark-1.5.0

#export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark

cd tests

echo "$NAME Generating the doctests test file"
python2.7 gendoct.py
GEN_DOCTESTS_SUCCESS=$?
if [[ $GEN_DOCTESTS_SUCCESS != 0 ]]
then
    echo "$NAME Generating doctests failed"
    exit 10
fi

#python2.7 -m pytest -s  # -s flag suppress io capture, such that we can see it during this run
python2.7 -m pytest

