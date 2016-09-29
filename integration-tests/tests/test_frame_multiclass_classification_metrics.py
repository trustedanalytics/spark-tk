# vim: set encoding=utf-8

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

from setup import tc, rm, get_sandbox_path
from sparktk.dtypes import float32
import logging
logger = logging.getLogger(__name__)


# Tests multi-class classification with string values
def test_multiclass_classification_metrics_001(tc):
    logger.info("create frame")
    rows = [["red", "red"],["blue", "green"],["green", "green"],["green", "green"],["orange","orange"],["red","orange"]]
    schema = [('labels', str),('predictions', str)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 4, "frame should have 6 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    logger.info("compute multiclass_classification_metrics()")
    cm = frame.multiclass_classification_metrics('labels', 'predictions', 1)

    assert(cm.f_measure, 0.6, "computed f_measure for this model should be equal to 0.6")
    assert(cm.recall, 0.666666666667, "computed recall for this model should be equal to 0.666666666667")
    assert(cm.accuracy, 0.666666666667, "computed accuracy for this model should be equal to 0.666666666667")
    assert(cm.precision, 0.638888888889, "computed precision for this model should be equal to 0.638888888889")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[1,0,0],[2,0,0],[0,1,0],[0,1,1]], "computed confusion_matrix for this models should be equal to [1,0,0],[2,0,0],[0,1,0],[0,1,1]")

    # Tests multi-class classification with float values and missing values
def test_multiclass_classification_metrics_002(tc):
    logger.info("create frame")
    rows = [[0.0, 0.0],[None, 0.0],[0.0, 0.0],[1.5, 1.5],[1.0, 1.0],[1.5, None]]
    schema = [('labels', float32),('predictions', float32)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 4, "frame should have 6 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    logger.info("compute multiclass_classification_metrics()")
    cm = frame.multiclass_classification_metrics('labels', 'predictions', 1)

    assert(cm.f_measure, 0.627777777778, "computed f_measure for this model should be equal to 0.627777777778")
    assert(cm.recall, 0.666666666667, "computed recall for this model should be equal to 0.666666666667")
    assert(cm.accuracy, 0.666666666667, "computed accuracy for this model should be equal to 0.666666666667")
    assert(cm.precision, 0.805555555556, "computed precision for this model should be equal to 0.805555555556")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[2,0,0],[0,1,0],[1,1,1]], "computed confusion_matrix for this models should be equal to [2,0,0],[0,1,0],[1,1,1]")
