#!/bin/sh
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

DIR="$( cd "$( dirname "$0" )" && pwd )"

echo $DIR

pwd

export HOSTNAME=`hostname`

# Create temporary directory for extracting the model, and add it to the library path
# It is difficult to modify the library path for dynamic libraries after the Java process has started
# LD_LIBRARY_PATH allows the OS to find the dynamic libraries and any dependencies
export MODEL_TMP_DIR=`mktemp -d -t tap-scoring-modelXXXXXXXXXXXXXXXXXX`


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MODEL_TMP_DIR


MODEL_SCORING_MAIN="org.trustedanalytics.scoring.MyMainFunction"


MODEL_SCORING_JAR=$(find `pwd` -name model-scoring*.jar)
echo $MODEL_SCORING_JAR

if [ -z $CP ]; then
    CP=$MODEL_SCORING_JAR:$DIR/../lib/*
fi

CMD=`echo java $@ -Dscoring-engine.tmpdir="$MODEL_TMP_DIR" -cp "$CP" $MODEL_SCORING_MAIN`
echo $CMD
$CMD

