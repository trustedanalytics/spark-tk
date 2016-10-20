#!/usr/bin/env bash
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


# builds the python documentation using pdoc

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"
cd $DIR

SPARKTK_DIR="$(dirname "$DIR")"

tmp_dir=`mktemp -d`
echo $NAME created temp dir $tmp_dir

cp -r $SPARKTK_DIR $tmp_dir
TMP_SPARKTK_DIR=$tmp_dir/sparktk

# skip documenting the doc
rm -r $TMP_SPARKTK_DIR/doc

echo $NAME pre-processing the python for the special doctest flags
python2.7 -m docgen -py=$TMP_SPARKTK_DIR


echo $NAME cd $TMP_SPARKTK_DIR
pushd $TMP_SPARKTK_DIR > /dev/null

TMP_SPARKTK_PARENT_DIR="$(dirname "$TMP_SPARKTK_DIR")"
TEMPLATE_DIR=$SPARKTK_DIR/doc/templates

# specify output folder:
HTML_DIR=$SPARKTK_DIR/doc/html
rm -rf $HTML_DIR

# call pdoc
echo $NAME PYTHONPATH=$TMP_SPARKTK_PARENT_DIR pdoc --only-pypath --html --html-dir=$HTML_DIR --template-dir $TEMPLATE_DIR --overwrite sparktk
PYTHONPATH=$TMP_SPARKTK_PARENT_DIR pdoc --only-pypath --html --html-dir=$HTML_DIR --template-dir $TEMPLATE_DIR --overwrite sparktk

popd > /dev/null

# convert a copy of the README.md to python and call pdoc to get the html version
echo $NAME convert README.md to python and run pdoc
(echo '"""'; tail -n +6 ../../../README.md; echo '"""') > readme.py
pdoc --html --html-no-source --overwrite readme.py
echo $NAME mv readme.m.html html/readme.m.html
mv readme.m.html html/readme.m.html

# Post-processing:  Patch the "Up" links
echo $NAME post-processing the HTML
python2.7 -m docgen -html=$HTML_DIR -main

echo $NAME cleaning up...
rm readme.py
rm readme.pyc
rm html/full/readme.m.html

echo $NAME rm $tmp_dir
rm -r $tmp_dir

echo $NAME Done.
