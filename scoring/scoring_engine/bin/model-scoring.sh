#!/bin/sh
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

