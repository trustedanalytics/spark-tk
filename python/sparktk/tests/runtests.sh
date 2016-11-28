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


# This script executes all of the tests located in this folder through the use
# of the nosetests api. Coverage is provided by coverage.py
# This script requires the installation of the install_pyenv.sh script

# '-x' to exclude dirs from coverage, requires nose-exclude to be installed

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"
echo "$NAME cd $DIR"
cd $DIR

export IN_UNIT_TESTS='true'

TESTS_DIR=$DIR
SPARKTK_DIR=`dirname $TESTS_DIR`
PYTHON_DIR=`dirname $SPARKTK_DIR`

echo TESTS_DIR=$TESTS_DIR
echo SPARKTK_DIR=$SPARKTK_DIR
echo PYTHON_DIR=$PYTHON_DIR

cd $PYTHON_DIR
export PYTHONPATH=$PYTHONPATH:$PYTHON_DIR
#python -c "import sys; print 'sys.path=' +  str(sys.path)"

# check if the python libraries are correctly installed by importing
# them through python. If there is no output then the module exists.
if [[ -e $(python2.7 -c "import sparktk") ]]; then
    echo "sparktk cannot be found"
    exit 1
fi

if [[ -e $(python2.7 -c "import coverage") ]]; then
    echo "Coverage.py is not installed into your python virtual environment please install coverage."
    exit 1
fi


rm -rf $PYTHON_DIR/cover

if [ "$1" = "-x" ] ; then
  EXCLUDE_DIRS_FILE=$TESTS_DIR/cov_exclude_dirs.txt
  if [[ ! -f $EXCLUDE_DIRS_FILE ]]; then
    echo ERROR: -x option: could not find exclusion file $EXCLUDE_DIRS_FILE
    exit 1
  fi
  echo -x option: excluding files from coverage described in $EXCLUDE_DIRS_FILE
  EXCLUDE_OPTION=--exclude-dir-file=$EXCLUDE_DIRS_FILE
fi

export COVERAGE_FILE=$DIR/../../../coverage/unit_test_coverage.dat
py.test --cov-config=$DIR/pycoverage.ini --cov=$SPARKTK_DIR --cov-report=html:$DIR/../../../coverage/pytest_unit $TESTS_DIR
echo "Coverage Report at:"
echo "$DIR/../../../pytest_unit"

unset IN_UNIT_TESTS
