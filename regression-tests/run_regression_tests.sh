#!/bin/bash

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"


export PYTHONPATH=$MAINDIR/regression-tests:$MAINDIR/python:$PYTHONPATH

export SPARKTK_HOME=$MAINDIR/sparktk-core/target

echo $NAME SPARKTK_HOME=$SPARKTK_HOME
echo $NAME PYTHONPATH=$PYTHONPATH

# Install datasets
$MAINDIR/regression-tests/automation/install_datasets.sh
# Run tests
py.test $MAINDIR/regression-tests
