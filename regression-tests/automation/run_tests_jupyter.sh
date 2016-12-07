#!/bin/bash

export SPARK_HOME="/usr/local/spark"
export SPARKTK_HOME="/home/vcap/jupyter/sparktk_test/sparktk-core/"

run_path="/home/vcap/jupyter/sparktk_test/regression-tests/sparktkregtests/testcases/frames"

pushd $run_path

py.test -vv $run_path/boxcox_test.py

popd
