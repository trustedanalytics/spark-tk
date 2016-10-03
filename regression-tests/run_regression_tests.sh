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
