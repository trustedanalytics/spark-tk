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

import logging
logger = logging.getLogger(__name__)


# Tests binary classification with string values
def test_binary_classification_metrics_001(tc):
    logger.info("create frame")
    rows = [["red", "red"],["blue", "green"],["green", "green"],["green", "green"]]
    schema = [('labels', str),('predictions', str)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 4, "frame should have 4 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    logger.info("compute binary_classification_metrics()")
    cm = frame.binary_classification_metrics('labels', 'predictions', 'green', 1)

    assert(cm.f_measure, 0.0, "computed f_measure for this model should be equal to 0.0")
    assert(cm.recall, 0.0, "computed recall for this model should be equal to 0.0")
    assert(cm.accuracy, 0.5, "computed accuracy for this model should be equal to 0.5")
    assert(cm.precision, 0.0, "computed precision for this model should be equal to 0.0")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[0, 2], [0, 2]], "computed confusion_matrix for this models should be equal to [[0, 2], [0, 2]]")

# Tests binary classification with float values
def test_binary_classification_metrics_002(tc):
    logger.info("create frame")
    rows = [[0.0, 0.0],[1.5, 0.0],[0.0, 0.0],[1.5, 1.5]]
    schema = [('labels', float),('predictions', float)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 4, "frame should have 4 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    logger.info("compute binary_classification_metrics()")
    cm = frame.binary_classification_metrics('labels', 'predictions', 1.5, 1)

    assert(cm.f_measure, 0.66666666666666663, "computed f_measure for this model should be equal to 0.66666666666666663")
    assert(cm.recall, 0.5, "computed recall for this model should be equal to 0.5")
    assert(cm.accuracy, 0.75, "computed accuracy for this model should be equal to 0.75")
    assert(cm.precision, 1.0, "computed precision for this model should be equal to 1.0")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[1, 1], [0, 2]], "computed confusion_matrix for this models should be equal to [[1, 1], [0, 2]]")

# Tests binary classification with data that includes missing values and having None as the positive label
def test_binary_classification_metrics_003(tc):
    logger.info("create frame")
    rows = [[0.0, 0.0],[1.5, None],[None, None],[1.5, 1.5]]
    schema = [('labels', float),('predictions', float)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 4, "frame should have 4 rows")
    assert(frame.column_names, ['labels', 'predictions'])

    logger.info("compute binary_classification_metrics()")
    cm = frame.binary_classification_metrics('labels', 'predictions', 1.5, 1)

    assert(cm.f_measure, 0.66666666666666663, "computed f_measure for this model should be equal to 0.66666666666666663")
    assert(cm.recall, 0.5, "computed recall for this model should be equal to 0.5")
    assert(cm.accuracy, 0.75, "computed accuracy for this model should be equal to 0.75")
    assert(cm.precision, 1.0, "computed precision for this model should be equal to 1.0")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[1, 1], [0, 2]], "computed confusion_matrix for this models should be equal to [[1, 1], [0, 2]]")

    logger.info("compute binary_classification_metrics() where the positive label is None.")
    cm = frame.binary_classification_metrics('labels', 'predictions', None)

    assert(cm.f_measure, 0.666666666667, "computed f_measure for this model should be equal to 0.666666666667")
    assert(cm.recall, 1.0, "computed recall for this model should be equal to 1.0")
    assert(cm.accuracy, 0.75, "computed accuracy for this model should be equal to 0.75")
    assert(cm.precision, 0.5, "computed precision for this model should be equal to 0.5")

    confusion_matrix = cm.confusion_matrix.values.tolist()
    assert(confusion_matrix, [[1, 0], [1, 2]], "computed confusion_matrix for this models should be equal to [[1, 0], [1, 2]]")
