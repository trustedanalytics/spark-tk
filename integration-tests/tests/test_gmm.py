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

def test_gmm(tc):

    logger.info("define schema")
    schema = [("Data", float),("Name", str)]

    logger.info("creating the frame")
    data = [[2, "ab"],
            [1,"cd"],
            [7,"ef"],
            [1,"gh"],
            [9,"ij"],
            [2,"kl"],
            [0,"mn"],
            [6,"op"],
            [5,"qr"],
            [6,'st'],
            [8,'uv'],
            [9,'wx'],
            [10,'yz']]

    f = tc.frame.create(data, schema=schema)
    logger.info(f.inspect())

    logger.info("training the model on the frame")
    model = tc.models.clustering.gmm.train(f, ['Data'], [1.0], 3, 99,seed=100)

    for g in model.gaussians:
        # mu should be a list[float]
        assert(all(isinstance(m, float) for m in g.mu))
        # sigma should be list[list[float]]
        for s_list in g.sigma:
            assert(all(isinstance(s, float) for s in s_list))

    logger.info("predicting the cluster using the model and the frame")
    model.predict(f)
    assert(set(f.column_names) == set(['Data', 'Name','predicted_cluster']))
    assert(len(f.column_names) == 3)
    assert(model.k == 3)
    rows = f.take(13)

    val = set(map(lambda y : y[2], rows))
    newlist = [[z[1] for z in rows if z[2] == a]for a in val]
    act_out = [[s.encode('ascii') for s in list] for list in newlist]
    act_out.sort(key = lambda rows: rows[0])
    
    #Providing seed value to test for a static result
    exp_out1 = [['ab', 'mn', 'cd', 'gh', 'kl'], ['ij', 'yz', 'uv', 'wx'], ['qr', 'ef', 'op', 'st']]
    result = False
    for list in act_out:
        if list not in exp_out1:
            result = False
        else:
            result = True
    assert(result == True)