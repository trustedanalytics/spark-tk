#!/usr/bin/env bash
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
python2.7 doctgen.py
GEN_DOCTESTS_SUCCESS=$?
if [[ $GEN_DOCTESTS_SUCCESS != 0 ]]
then
    echo "$NAME Generating doctests failed"
    exit 10
fi

#enable to debug
#export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=n,address=atk-wse.jf.intel.com:5005,suspend=y

#python2.7 -m pytest -s  # -s flag suppress io capture, such that we can see it during this run
#python2.7 -m pytest -k test_kmeans  # example to run individual test
#python2.7 -m pytest -k test_docs_python_sparktk_frame_ops_drop_columns_py  # example to run individual doc test
export COVERAGE_FILE=$DIR/../coverage/integration_test_coverage.dat
py.test --cov-config=$DIR/pycoverage.ini --cov=$DIR/../python --cov-report=html:$DIR/../coverage/pytest_integration $@
