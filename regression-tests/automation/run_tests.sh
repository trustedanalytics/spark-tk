#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"


export PYTHONPATH=$MAINDIR/regression-tests:/opt/cloudera/parcels/CDH/lib/spark/python/pyspark:/usr/lib/python2.7/site-packages/:$MAINDIR/graphframes:$PYTHONPATH

export SPARKTK_HOME=$MAINDIR/regression-tests/automation/sparktk-core/

echo "spark tk home"
echo $SPARKTK_HOME

py.test --boxed -n24 $MAINDIR/regression-tests
