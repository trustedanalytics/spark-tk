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

"""Tests Linear Regression Model against known values"""
import unittest
from sparktkregtests.lib import sparktk_test


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

    @unittest.skip("no publish") 
    def test_model_publish(self):
        """Test publishing a linear regression model"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'])
        model_path = model.publish()
        self.assertIn("hdfs", model_path)
        self.assertIn("tar", model_path)

    def test_model_test(self):
        """Test test functionality"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'])
        output = model.test(self.frame, "label")
        self.assertAlmostEqual(
            model.mean_squared_error, output.mean_squared_error)
        self.assertAlmostEqual(
            model.root_mean_squared_error, output.root_mean_squared_error)
        self.assertAlmostEqual(
            model.mean_absolute_error, output.mean_absolute_error)
        self.assertAlmostEqual(
            model.explained_variance, output.explained_variance)

    def test_model_predict_output(self):
        """Test output format of predict"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'])
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def test_model_elastic_net(self):
        """Test elastic net argument"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
            elastic_net_parameter=0.3)
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def test_model_fix_intercept(self):
        """Test fix intercept argument"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
            fit_intercept=False)
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def test_model_max_iterations(self):
        """Test max iterations argument"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          max_iterations=70)
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def test_model_reg_param(self):
        """Test regularization parameter argument"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          reg_param=0.000000002)
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def test_model_standardization(self):
        """Test test non-standardized data"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          standardization=False)
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def test_model_tolerance(self):
        """Test test a different model tolerance"""
        model = self.context.models.regression.linear_regression.train(
            self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          tolerance=0.0000000000000000001)
        predict = model.predict(self.frame, ['c1', 'c2', 'c3', 'c4'])
        self._validate_results(model, predict)

    def _validate_results(self, res, predict):
        # validate dictionary entries, weights, and predict results
        self.assertAlmostEqual(res.mean_absolute_error, 0.0)
        self.assertAlmostEqual(res.root_mean_squared_error, 0.0)
        self.assertAlmostEqual(res.mean_squared_error, 0.0)
        self.assertAlmostEqual(res.intercept, 0.0)
        self.assertEqual(res.value_column, "label")
        self.assertItemsEqual(
            res.observation_columns, ['c1', 'c2', 'c3', 'c4'])
        self.assertLess(res.iterations, 150)

        for (i, j) in zip([0.5, -0.7, -0.24, 0.4], res.weights):
            self.assertAlmostEqual(i, j, places=4)

        pd_res = predict.to_pandas(predict.count())
        for index, row in pd_res.iterrows():
            self.assertAlmostEqual(row["label"], row["predicted_value"], places=4)


if __name__ == '__main__':
    unittest.main()
