#!/usr/bin/env bash

# runs all the integration tests

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"

if [ -z "$PYSPARK_PYTHON" ]; then
    export PYSPARK_PYTHON=/usr/bin/python2.7
fi
echo $NAME PYSPARK_PYTHON=$PYSPARK_PYTHON

if [ -z "$PYSPARK_DRIVER_PYTHON" ]; then
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python2.7
fi
echo $NAME PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON

if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
fi
echo $NAME SPARK_HOME=$SPARK_HOME


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
