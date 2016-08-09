#!/bin/bash

# Create a directory for the datasets as the current user
# Install the datasets as the current user

NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

target=/user/$USER/qa_data/

sudo -u hdfs hdfs dfs -mkdir -p $target
sudo -u hdfs hdfs dfs -chown $USER:$USER $target

hdfs dfs -put -f $DIR/../datasets/* $target
