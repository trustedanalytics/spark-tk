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


# Tests power iteration clustering with initialization_mode='degree'
def test_power_iteration_clustering_001(tc):
    logger.info("create frame")
    rows = [[1,2,1.0],[1,3,0.3],[2,3,0.3],[3,0,0.03],[0,5,0.01],[5,4,0.3],[5,6,1.0],[4,6,0.3]]
    schema = [('src', int),('dest', int),('similarity', float)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 8, "frame should have 8 rows")
    assert(frame.column_names, ['src', 'dest', 'similarity'])

    logger.info("compute power_iteration_clustering()")
    cm = frame.power_iteration_clustering('src', 'dest', 'similarity', k=3, max_iterations=99, initialization_mode='degree')
    assert(cm[1], 3, "computed number of clusters for this model should be 3")
    assert(cm[2], {u'2': 1, u'3': 4, u'1': 2}, "computed cluster map for this model should be {u'2': 1, u'3': 4, u'1': 2}")

# Tests power iteration clustering with initialization_mode='random'
def test_power_iteration_clustering_002(tc):
    logger.info("create frame")
    rows = [[1,2,1.0],[1,3,0.3],[2,3,0.3],[3,0,0.03],[0,5,0.01],[5,4,0.3],[5,6,1.0],[4,6,0.3]]
    schema = [('src', int),('dest', int),('similarity', float)]
    frame = tc.frame.create(rows, schema)

    assert(frame.count(), 8, "frame should have 8 rows")
    assert(frame.column_names, ['src', 'dest', 'similarity'])

    logger.info("compute power_iteration_clustering()")
    cm = frame.power_iteration_clustering('src', 'dest', 'similarity', k=3, max_iterations=99, initialization_mode='random')
    assert(cm[1], 3, "computed number of clusters for this model should be 3")
    assert(cm[2], {u'2': 1, u'3': 4, u'1': 2}, "computed cluster map for this model should be {u'2': 1, u'3': 4, u'1': 2}")
