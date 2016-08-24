#!/usr/bin/env bash

# runs all the integration tests

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"
echo "$NAME cd $DIR"
cd $DIR


if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
fi
echo $NAME SPARK_HOME=$SPARK_HOME

if [ -z "$HADOOP_CONF_DIR" ]; then
    export HADOOP_CONF_DIR="."
fi
echo $NAME HADOOP_CONF_DIR=$HADOOP_CONF_DIR

echo "$NAME Calling clean.sh"
./clean.sh

cd tests

echo "$NAME Generating the doctests test file"
python2.7 gendoct.py
GEN_DOCTESTS_SUCCESS=$?
if [[ $GEN_DOCTESTS_SUCCESS != 0 ]]
then
    echo "$NAME Generating doctests failed"
    exit 10
fi

#enable to debug
#export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=n,address=atk-wse.jf.intel.com:5005,suspend=y

# todo - allow these pytest flags to be passed in to this script
#python2.7 -m pytest -s  # -s flag suppress io capture, such that we can see it during this run
#python2.7 -m pytest -k test_kmeans  # example to run individual test
#python2.7 -m pytest -k test_docs_python_sparktk_frame_ops_drop_columns_py  # example to run individual doc test
python2.7 -m pytest --junitxml=pytest-report $@
