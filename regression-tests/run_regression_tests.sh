#!/bin/bash
NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"


export PYTHONPATH=$MAINDIR/regression-tests:$PYTHONPATH

export SPARKTK_HOME=$MAINDIR/sparktk-core/target

echo "spark tk home"
echo $SPARKTK_HOME
echo $PYTHONPATH

# Install datasets
$MAINDIR/regression-tests/automation/install_datasets.sh
# Run tests
py.test $MAINDIR/regression-tests
