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

def test_naive_bayes(tc):

    logger.info("define schema")
    schema = [("Class", int),("Dim_1", int),("Dim_2", int),("Dim_3", int)]

    logger.info("creating the frame")
    data = [[0,1,0,0],
            [2,0,0,0],
            [1,0,1,0],
            [1,0,2,0],
            [2,0,0,1],
            [2,0,0,2]]
    f = tc.frame.create(data, schema=schema)
    logger.info(f.inspect())

    logger.info("training the model on the frame")
    model = tc.models.classification.naive_bayes.train(f, 'Class', ['Dim_1', 'Dim_2', 'Dim_3'])
    logger.info("predicting the class using the model and the frame")
    predicted_frame = model.predict(f)
    assert(set(predicted_frame.column_names) == set(['Class', 'Dim_1', 'Dim_2', 'Dim_3','predicted_class']))
    assert(len(predicted_frame.column_names) == 5)
    assert(len(f.column_names) == 4)
    metrics = model.test(predicted_frame, 'Class')
    assert(metrics.accuracy == 1.0)
    assert(metrics.f_measure == 1.0)
    assert(metrics.precision == 1.0)
    assert(metrics.recall == 1.0)


