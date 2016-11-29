#!/bin/bash

wget $GRAHPFRAMES -O $BASE_DIR/graphframes.jar
unzip -o $BASE_DIR/graphframes.jar "graphframes/*" $BASE_DIR/
ls -la $BASE_DIR/graphframes
cp -Rv $BASE_DIR/graphframes $PYTHON_ENV_DIR/local/lib/python2.7/site-packages/
