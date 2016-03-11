#!/usr/bin/env bash

export PYSPARK_PYTHON=/usr/bin/python2.7
export PYSPARK_DRIVER_PYTHON=/usr/bin/python2.7
#export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export SPARK_HOME=/home/blbarker/spark-1.5.0

cd tests
python2.7 -m pytest -s  # -s flag suppress io capture, such that we can see it during this run
#python2.7 -m pytest

