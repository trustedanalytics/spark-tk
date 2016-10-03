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

##Install the sparktk-core python package and very the users system is ready sparktk.
##Warnings will be displayed for missing pacakages or ENV variables.
## - see if SPARK_HOME is set
##  - if the SPARK_HOME is set verify spark version
## - install sparktk pip package
## -

EXIT=0
SPARK_VERSION=1.6.0

if [ -f $SPARK_HOME ]; then
    echo "Your SPARK_HOME variable isn't set. Please set SPARK_HOME to the root of your spark installation."
    EXIT=1
else
    echo "Verifying Spark version"
    SPARK_VERSION=$SPARK_VERSION bash -c " $SPARK_HOME/bin/spark-shell --conf spark.master=local -i version.scala 2> /dev/null"
    if [ $? -ne 0 ]; then
        echo "SPARK version mismatch. This version of sparktk requires $SPARK_VERSION."
        EXIT=1
    fi
fi

if [ $EXIT -ne 0 ]; then
    echo "Please review and correct any errors before using spark-tk."
fi

graphframes=$(find `pwd`/lib/ -name "graphframes*.jar")
unzip -o $graphframes "graphframes/*" .
cp python/setup.py graphframes/

pip install -U graphframes/

pip install -U python/sparktk*.tar.gz





exit $EXIT