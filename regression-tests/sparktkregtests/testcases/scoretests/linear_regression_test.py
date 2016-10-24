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

""" Tests Linear Regression scoring engine """
import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import scoring_utils


class LinearRegression(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(LinearRegression, self).setUp()
        dataset = self.get_file("linear_regression_gen.csv")
        schema = [("c1", float),
                  ("c2", float),
                  ("c3", float),
                  ("c4", float),
                  ("label", float)]

        self.frame = self.context.frame.import_csv(
            dataset, schema=schema)

    def test_model_publish(self):
        """Test publishing a linear regression model"""
        model = self.context.models.regression.linear_regression.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'])

        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        test_rows = predict.to_pandas(predict.count())

        file_name = self.get_name("linear_regression")
        model_path = model.export_to_mar(self.get_export_file(file_name))
        with scoring_utils.scorer(model_path) as scorer:
            for _, i in test_rows.iterrows():
                res = scorer.score(
                    [dict(zip(["c1", "c2", "c3", "c4"], list(i[0:4])))])
                self.assertEqual(
                    i["predicted_value"], res.json()["data"][0]['Prediction'])

            


if __name__ == '__main__':
    unittest.main()
