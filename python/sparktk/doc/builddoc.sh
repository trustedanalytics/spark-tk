#!/usr/bin/env bash

# builds the python documentation using pdoc


NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"

SPARKTK_DIR="$(dirname "$DIR")"

tmp_dir=`mktemp -d`
echo $NAME created temp dir $tmp_dir

cp -r $SPARKTK_DIR $tmp_dir
TMP_SPARKTK_DIR=$tmp_dir/sparktk

# skip documenting the doc
rm -r $TMP_SPARKTK_DIR/doc

echo $NAME pre-processing the python for the special doctest flags
python2.7 -m doctools -py=$TMP_SPARKTK_DIR


echo $NAME cd $TMP_SPARKTK_DIR
pushd $TMP_SPARKTK_DIR > /dev/null

TMP_SPARKTK_PARENT_DIR="$(dirname "$TMP_SPARKTK_DIR")"
TEMPLATE_DIR=$SPARKTK_DIR/doc/templates

# specify output folder:
HTML_DIR=$SPARKTK_DIR/doc/html

# call pdoc
echo $NAME PYTHONPATH=$TMP_SPARKTK_PARENT_DIR pdoc --only-pypath --html --html-dir=$HTML_DIR --template-dir $TEMPLATE_DIR --overwrite sparktk
PYTHONPATH=$TMP_SPARKTK_PARENT_DIR pdoc --only-pypath --html --html-dir=$HTML_DIR --template-dir $TEMPLATE_DIR --overwrite sparktk

popd > /dev/null

# Post-processing:  Patch the "Up" links
echo $NAME post-processing the HTML
python2.7 -m doctools -html=$HTML_DIR

echo $NAME cleaning up...
echo $NAME rm $tmp_dir
rm -r $tmp_dir

echo $NAME Done.
