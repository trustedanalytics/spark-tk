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

NAME="[`basename $BASH_SOURCE[0]`]"
DIR="$( cd "$( dirname "$BASH_SOURCE[0]" )" && pwd )"
echo "$NAME DIR=$DIR"

MAINDIR="$(dirname $DIR)"
MAINDIR="$(dirname $MAINDIR)"
MAINDIR="$(dirname $MAINDIR)"

scoring_model=$1

pushd $MAINDIR/scoring/scoring_model
./bin/model-scoring.sh -Dtrustedanalytics.scoring-engine.archive-mar=$scoring_model -Dtrustedanalytics.scoring.port=9100 > $MAINDIR/scoring_out.log 2> $MAINDIR/scoring_error.log
popd
