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

import unittest
import os
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils
from ConfigParser import SafeConfigParser


class PrincipalComponent(sparktk_test.SparkTKTestCase):

    def setUp(self):
        super(PrincipalComponent, self).setUp()
        schema = [("X1", int),
                  ("X2", int),
                  ("X3", int),
                  ("X4", int),
                  ("X5", int),
                  ("X6", int),
                  ("X7", int),
                  ("X8", int),
                  ("X9", int),
                  ("X10", int)]
        pca_traindata = self.get_file("pcadata.csv")
        self.frame = self.context.frame.import_csv(pca_traindata, schema=schema)
        self.config = SafeConfigParser()
        filepath = os.path.abspath(os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "..", "..", "lib", "port.ini"))

        self.config.read(filepath)

    def test_model_scoring(self):
        """Test pca scoring"""
        model = self.context.models.dimreduction.pca.train(
            self.frame,
            ["X1", "X2", "X3", "X4", "X5",
            "X6", "X7", "X8", "X9", "X10"],
            False, 10)

        file_name = self.get_name("pca")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(
                model_path, self.config.get('port', self.id())) as scorer:
            baseline = model.predict(self.frame, mean_centered=False)
            testvals = baseline.to_pandas(50)

            for _, i in testvals.iterrows():
                r = scorer.score(
                    [dict(zip(["X1", "X2", "X3", "X4", "X5",
                               "X6", "X7", "X8", "X9", "X10"],
                    map(lambda x: x, i[0:10])))])
                map(lambda x, y: self.assertAlmostEqual(float(x),float(y)),
                    r.json()["data"][-1]["principal_components"], i[10:])


if __name__ == '__main__':
    unittest.main()
