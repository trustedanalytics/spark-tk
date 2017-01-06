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

def test_svm(tc):

    logger.info("define schema")
    schema = [("data", float),("label", str)]

    logger.info("creating the frame")
    data = [[-48,1],
            [-75,1],
            [-63,1],
            [-57,1],
            [73,0],
            [-33,1],
            [100,0],
            [-54,1],
            [78,0],
            [48,0],
            [-55,1],
            [23,0],
            [45,0],
            [75,0],
            [95,0],
            [73,0],
            [7,0],
            [39,0],
            [-60,1]]

    f = tc.frame.create(data, schema=schema)
    logger.info(f.inspect())

    logger.info("training the model on the frame")
    model = tc.models.classification.svm.train(f, ['data'], 'label')
    logger.info("predicting the class using the model and the frame")
    predicted_frame = model.predict(f)
    assert(set(predicted_frame.column_names) == set(['data', 'label', 'predicted_label']))
    assert(len(predicted_frame.column_names) == 3)
    assert(len(f.column_names) == 2)
    metrics = model.test(predicted_frame, 'label')
    assert(metrics.accuracy == 1.0)
    assert(metrics.f_measure == 1.0)
    assert(metrics.precision == 1.0)
    assert(metrics.recall == 1.0)
