#!/usr/bin/env bash

# Launches PySpark shell enabled to run spark-tk

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "$NAME"
echo "$NAME  ************* gopyspark.sh is not preferred ***************"
echo "$NAME  **"
echo "$NAME  **  Use regular python session and create TkContext instead"
echo "$NAME  **"
echo "$NAME  **  >>> import sparktk as tk"
echo "$NAME  **  >>> tc = tk.TkContext()  # You can pass more args here"
echo "$NAME  **"
echo "$NAME  ***********************************************************"
echo "$NAME"
echo "$NAME DIR=$DIR"

# run python code to set things up
EXPORT_CMDS=`python2.7 - <<END
import sparktk.sparkconf as conf
from sparktk.zip import zip_sparktk

conf.set_env_for_sparktk()
#conf.set_env_for_sparktk(debug=5005)  # <-- **Here's the line to turn on debugging**

conf.print_bash_cmds_for_sparktk_env()
zip_path = zip_sparktk()
print "export SPARKTK_PYZIP=%s" % zip_path
END`

#echo "$NAME EXPORT_CMDS=$EXPORT_CMDS"

# the python code prints a bunch of export cmds, so we need to source them
source /dev/stdin < <(echo $EXPORT_CMDS)

echo "$NAME SPARKTK_HOME=$SPARKTK_HOME"
echo "$NAME SPARK_HOME=$SPARK_HOME"
#echo "$NAME SPARKTK_PYZIP=$SPARKTK_PYZIP"

if [ -z "$PYSPARK_BIN" ]; then
    export PYSPARK_BIN=pyspark
fi
echo $NAME PYSPARK_BIN=$PYSPARK_BIN

if [ -z "$PYSPARK_MASTER" ]; then
    export PYSPARK_MASTER=local[4]
fi
echo $NAME PYSPARK_MASTER=$PYSPARK_MASTER


echo IPYTHON=1 $PYSPARK_BIN --master $PYSPARK_MASTER $PYSPARK_SUBMIT_ARGS
IPYTHON=1 $PYSPARK_BIN --master $PYSPARK_MASTER $PYSPARK_SUBMIT_ARGS
