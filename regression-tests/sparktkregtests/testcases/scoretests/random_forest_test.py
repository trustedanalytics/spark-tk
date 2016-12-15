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


""" test cases for random forest"""
import unittest
import os
from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class RandomForest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForest, self).setUp()

        schema = [("feat1", int), ("feat2", int), ("class", str)]
        filename = self.get_file("rand_forest_class.csv")

        self.frame = self.context.frame.import_csv(filename, schema=schema)

    def test_class_scoring(self):
        """Test random forest classifier scoring model"""
        rfmodel = self.context.models.classification.random_forest_classifier.train(
            self.frame, ["feat1", "feat2"], "class", seed=0)
        
        result_frame = rfmodel.predict(self.frame)
        preddf = result_frame.to_pandas(result_frame.count())

        file_name = self.get_name("random_forest_classifier")
        model_path = rfmodel.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(
                model_path, self.id()) as scorer:
            for i, row in preddf.iterrows():
                res = scorer.score(
                    [dict(zip(["feat1", "feat2"],
                    map(lambda x: x,row[0:2])))])
                self.assertAlmostEqual(
                    float(row[3]), float(res.json()["data"][0]['PredictedClass']))

    def test_reg_scoring(self):
        """Test random forest regressor scoring  model"""
        rfmodel = self.context.models.regression.random_forest_regressor.train(
            self.frame, ["feat1", "feat2"], "class", seed=0)

        predresult = rfmodel.predict(self.frame)
        preddf = predresult.to_pandas(predresult.count())

        file_name = self.get_name("random_forest_regressor")
        model_path = rfmodel.export_to_mar(self.get_export_file(file_name))

        with scoring_utils.scorer(
                model_path, self.id()) as scorer:
            for i, row in preddf.iterrows():
                res = scorer.score(
                    [dict(zip(["feat1", "feat2"], map(lambda x: x,row[0:2])))])

                self.assertAlmostEqual(
                    float(row[3]), float(res.json()["data"][0]['Prediction']))


if __name__ == '__main__':
    unittest.main()
