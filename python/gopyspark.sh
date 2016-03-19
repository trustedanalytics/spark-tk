#!/usr/bin/env bash

# Launches PySpark shell enabled to run spark-tk

# todo - move to a bin folder

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"

export SPARK_TK_DIR=$DIR/../spark-tk
echo "$NAME SPARK_TK_DIR=$SPARK_TK_DIR"

export SPARK_TK_JAR=$SPARK_TK_DIR/target/*
export SPARK_TK_DEP_JARS=$SPARK_TK_DIR/target/dependencies/*

if [ -z "$PYSPARK_BIN" ]; then
    #export PYSPARK_BIN=pyspark
    export PYSPARK_BIN=~/spark-1.5.0/bin/pyspark
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

#IPYTHON=1 $PYSPARK_BIN --master local[4] --jars $SPARK_TK_JAR,$SPARK_TK_DEP_JARS --driver-class-path "$SPARK_TK_JAR:$SPARK_TK_DEP_JARS"
echo $NAME IPYTHON=1 $PYSPARK_BIN --master $PYSPARK_MASTER --driver-class-path "$SPARK_TK_JAR:$SPARK_TK_DEP_JARS"
IPYTHON=1 $PYSPARK_BIN --master local[4] --driver-class-path "$SPARK_TK_JAR:$SPARK_TK_DEP_JARS"


