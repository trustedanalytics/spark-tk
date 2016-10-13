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


# Launches PySpark shell enabled to run spark-tk

# todo - move to a bin folder

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"

export CORE_DIR=$DIR/../core
echo "$NAME SPARK_TK_DIR=$CORE_DIR"

export SPARK_TK_JAR=$CORE_DIR/target/*
export SPARK_TK_DEP_JARS=$CORE_DIR/target/dependencies/*

# todo - build the jars argument as well, until then, this probably won't work on real cluster

if [ -z "$SPARK_HOME" ]; then
    export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
fi
echo $NAME SPARK_HOME=$SPARK_HOME

if [ -z "$PYSPARK_BIN" ]; then
    export PYSPARK_BIN=pyspark
fi
echo $NAME PYSPARK_BIN=$PYSPARK_BIN

if [ -z "$PYSPARK_MASTER" ]; then
    export PYSPARK_MASTER=local[4]
fi
echo $NAME PYSPARK_MASTER=$PYSPARK_MASTER

if [ -z "$PYSPARK_PYTHON" ]; then
    export PYSPARK_PYTHON=/usr/bin/python2.7
fi
echo $NAME PYSPARK_PYTHON=$PYSPARK_PYTHON

if [ -z "$PYSPARK_DRIVER_PYTHON" ]; then
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python2.7
fi
echo $NAME PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON

#enable to debug
export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

echo $NAME IPYTHON=1 $PYSPARK_BIN --master $PYSPARK_MASTER --driver-class-path "$SPARK_TK_JAR:$SPARK_TK_DEP_JARS"
IPYTHON=1 $PYSPARK_BIN --master $PYSPARK_MASTER --driver-class-path "$SPARK_TK_JAR:$SPARK_TK_DEP_JARS"
