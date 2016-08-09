#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"


export PYTHONPATH=$MAINDIR/regression-tests/sparkregtests:$PYTHONPATH

export SPARKTK_HOME=$MAINDIR/core/target

echo "spark tk home"
echo $SPARKTK_HOME
echo $PYTHONPATH

py.test $MAINDIR/regression-tests
