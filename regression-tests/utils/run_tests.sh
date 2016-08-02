#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"



export SPARKTK_HOME=$MAINDIR/sparktk_jars/spark-tk/

echo "spark tk home"
echo $SPARKTK_HOME

py.test $MAINDIR/regression-tests
