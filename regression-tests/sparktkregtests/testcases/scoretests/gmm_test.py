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

# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

""" Tests GMM scoring engine """
import unittest
import os
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class GMM(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(GMM, self).setUp()
        data_file = self.get_file("gmm_data.csv")
        self.frame = self.context.frame.import_csv(
            data_file, schema=[("x1", float), ("x2", float)])

    def test_model_scoring(self):
        """Test publishing a gmm model"""
        model = self.context.models.clustering.gmm.train(
                    self.frame, ["x1", "x2"],
                    column_scalings=[1.0, 1.0],
                    k=5,
                    max_iterations=500,
                    seed=20,
                    convergence_tol=0.0001)

        predict = model.predict(self.frame)
        test_rows = predict.to_pandas(predict.count())

        file_name = self.get_name("gmm")
        model_path = model.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(
                model_path, self.id()) as scorer:
            for i, row in test_rows.iterrows():
                res = scorer.score(
                    [dict(zip(["x1", "x2"], list(row[0:2])))])
                self.assertEqual(
                    row["predicted_cluster"], res.json()["data"][0]['Score'])

if __name__ == '__main__':
    unittest.main()
