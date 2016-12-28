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


def test_grid_search(tc):

    logger.info("define schema")
    schema = [("data", float),("label", int)]

    logger.info("creating the frame")
    data = [[1,0],
            [2,0],
            [3,0],
            [4,0],
            [5,0],
            [6,1],
            [7,1],
            [8,1],
            [9,1],
            [10,1]]

    f = tc.frame.create(data, schema=schema)
    logger.info(f.inspect())

    logger.info("running grid search for two models and two parameter configurations per model")
    grid_result = tc.models.grid_search(f, f, [(tc.models.classification.svm,
                                                {"observation_columns":"data",
                                                 "label_column":"label",
                                                 "num_iterations": tc.models.grid_values(2, 10),
                                                 "step_size": 0.01}),
                                               (tc.models.classification.logistic_regression,
                                                {"observation_columns":"data",
                                                 "label_column":"label",
                                                 "num_iterations": tc.models.grid_values(2, 10),
                                                 "step_size": 0.01})])
    logger.info("finding the number of models evaluated")
    assert(len(grid_result.grid_points) == 4 )
    logger.info("finding the best model and its parameters")
    best = grid_result.find_best().metrics
    assert(best.accuracy == 1.0)
    assert(best.recall == 1.0)
    assert(best.precision == 1.0)
    assert(best.f_measure == 1.0)
