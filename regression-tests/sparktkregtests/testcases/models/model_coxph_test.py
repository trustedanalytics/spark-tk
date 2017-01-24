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

""" test CoxPH  model """
import unittest
from numpy import exp

from sparktkregtests.lib import sparktk_test


class CoxPH(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(CoxPH, self).setUp()

        dataset = self.get_file("multi_cox_5c_1kr.csv")

        self.schema = [("x1", float),
                       ("x2", float),
                       ("x3", float),
                       ("time", float),
                       ("censor", float)]
        self.frame = self.context.frame.import_csv(
            dataset, schema=self.schema, header=True)

    def test_train_default(self):
        """ Tests default coxPH model """
        model = self.context.models.survivalanalysis.cox_ph.train(
            self.frame, "time", ["x1", "x2", "x3"], "censor")

        expected_betas = [0.001329, -0.002289, -0.000833]
        expected_means = [74.5, 9.517, 57.58]

        for i, value in enumerate(model.beta):
            self.assertAlmostEqual(
                value, expected_betas[i], delta=0.0001)
            self.assertAlmostEqual(
                model.mean[i], expected_means[i], delta=0.01)

    def test_predict_multivariate(self):
        """ Tests predicted hazard ratios on mulitvariate data """
        model = self.context.models.survivalanalysis.cox_ph.train(
            self.frame, "time", ["x1", "x2", "x3"], "censor")
        predict_frame = model.predict(self.frame)
        self._validate_predictions(model, predict_frame)

    def test_predict_frame_comparison(self):
        """Tests predicted hazard ratios using frame comparison"""
        model = self.context.models.survivalanalysis.cox_ph.train(
            self.frame, "time", ["x1", "x2", "x3"], "censor")

        data = [[66, 6, 36, 2, 1], [87, 0, 88, 3, 1], [90, 13, 26, 5, 1]]
        comp_frame = self.context.frame.create(data, schema=self.schema)
        predict_frame = model.predict(
            frame=self.frame, comparison_frame=comp_frame)

        # update mean to comparison frame's mean
        correct_mean = [81.0, 6.33333333333, 50.0]
        self._validate_predictions(model, predict_frame, correct_mean)

    def test_univariate_coxPH(self):
        """Tests training and prediction on univariate data"""
        model = self.context.models.survivalanalysis.cox_ph.train(
            self.frame, "time", ["x1"], "censor")

        # test trained parameters
        self.assertAlmostEqual(model.mean[0], 74.5, delta=0.01)
        self.assertAlmostEqual(model.beta[0], 0.00122, delta=.0001)

        # test predicted hazard ratios
        predict_frame = model.predict(self.frame)
        df_predict = predict_frame.to_pandas(predict_frame.count())
        for i, row in df_predict.iterrows():
            self.assertAlmostEqual(
                exp((row['x1'] - model.mean[0])*model.beta[0]),
                row['hazard_ratio'])

    def test_max_steps(self):
        """ Tests whether coxPH model converges in given max_steps"""
        model = self.context.models.survivalanalysis.cox_ph.train(
            self.frame, "time", ["x1", "x2", "x3"], "censor", max_steps=30)

        expected_betas = [0.001329, -0.002289, -0.000833]
        expected_means = [74.5, 9.517, 57.58]

        for i, value in enumerate(model.beta):
            self.assertAlmostEqual(
                value, expected_betas[i], delta=0.0001)
            self.assertAlmostEqual(
                model.mean[i], expected_means[i], delta=0.01)

        # test predicted hazard ratios
        predict_frame = model.predict(self.frame)
        self._validate_predictions(model, predict_frame)

    def test_convergence_tolerance(self):
        """ Tests convergence tolerance """
        model = self.context.models.survivalanalysis.cox_ph.train(
            self.frame, "time", ["x1", "x2", "x3"],
            "censor", convergence_tolerance=0.000001)

        expected_betas = [0.001329, -0.002289, -0.000833]
        expected_means = [74.5, 9.517, 57.58]

        for i, value in enumerate(model.beta):
            self.assertAlmostEqual(
                value, expected_betas[i], delta=0.0001)
            self.assertAlmostEqual(
                model.mean[i], expected_means[i], delta=0.01)

        # test predicted hazard ratios
        predict_frame = model.predict(self.frame)
        self._validate_predictions(model, predict_frame)

    def test_bad_feature_col_name(self):
        """ Tests behavior for bad feature column name """
        with self.assertRaisesRegexp(
                Exception, "Invalid column name ERR .*"):
            self.context.models.survivalanalysis.cox_ph.train(
                self.frame, "time", ["ERR", "x2", "x3"], "censor")

    def test_bad_time_col_name(self):
        """ Tests behavior for bad time column name """
        with self.assertRaisesRegexp(
                Exception, "Invalid column name ERR .*"):
            self.context.models.survivalanalysis.cox_ph.train(
                self.frame, "ERR", ["x1", "x2", "x3"], "censor")

    def test_missing_time_col(self):
        """ Tests behavior for missing time column """
        with self.assertRaisesRegexp(
                Exception, "train\(\) takes at least 4 arguments.*"):
            self.context.models.survivalanalysis.cox_ph.train(
                self.frame, ["x1", "x2", "x3"], censor_column="censor")

    def test_missing_censor_col(self):
        """ Tests behavior for missing censor column """
        with self.assertRaisesRegexp(
                Exception, "train\(\) got multiple values .*"):
            self.context.models.survivalanalysis.cox_ph.train(
                self.frame, ["x1", "x2", "x3"], time_column="time")

    def test_train_with_censor(self):
        """ Tests trained betas with censoring of data """
        dataset = [[78, 8, 64, 0, 0],
                   [90, 16, 70, 1, 0],
                   [76, 6, 36, 2, 1],
                   [99, 0, 88, 3, 1],
                   [69, 5, 72, 4, 0],
                   [84, 13, 26, 5, 1],
                   [99, 19, 67, 6, 0],
                   [50, 2, 94, 7, 0],
                   [68, 17, 41, 8, 1],
                   [71, 1, 64, 9, 1]]

        schema = [("x1", float),
                  ("x2", float),
                  ("x3", float),
                  ("time", int),
                  ("censor", int)]
        frame = self.context.frame.create(
            dataset, schema=schema)

        model = self.context.models.survivalanalysis.cox_ph.train(
            frame, "time", ["x1", "x2", "x3"], "censor")

        expected_betas = [0.09663986169721611,
                          -0.12223224761197181,
                          -0.05931281683739255]
        expected_means = [78.4, 8.7, 62.2]

        for i, value in enumerate(model.beta):
            self.assertAlmostEqual(
                value, expected_betas[i], delta=0.0001)
            self.assertAlmostEqual(
                model.mean[i], expected_means[i], delta=0.01)

    def test_predict_with_censor(self):
        """Tests correctness of hazard ratios with censored data"""
        dataset = [[78, 8, 64, 0, 0],
                   [90, 16, 70, 1, 0],
                   [76, 6, 36, 2, 1],
                   [99, 0, 88, 3, 1],
                   [69, 5, 72, 4, 0],
                   [84, 13, 26, 5, 1],
                   [99, 19, 67, 6, 0],
                   [50, 2, 94, 7, 0],
                   [68, 17, 41, 8, 1],
                   [71, 1, 64, 9, 1]]

        schema = [("x1", float),
                  ("x2", float),
                  ("x3", float),
                  ("time", int),
                  ("censor", int)]
        frame = self.context.frame.create(
            dataset, schema=schema)

        model = self.context.models.survivalanalysis.cox_ph.train(
            frame, "time", ["x1", "x2", "x3"], "censor")
        predict_frame = model.predict(frame)
        self._validate_predictions(model, predict_frame)

    def _validate_predictions(self, model, predict_frame, correct_mean=None):
        """Validates predicted hazard ratios"""
        pred_results = predict_frame.to_pandas(
            predict_frame.count()).values.tolist()
        mean = model.mean if correct_mean is None else correct_mean

        actual_ratios = [row[-1] for row in pred_results]
        expected_ratios = [exp(sum([model.beta[i] * (val-mean[i])
                           for i, val in enumerate(row[:-3])]))
                           for row in pred_results]

        for actual, expected in zip(actual_ratios, expected_ratios):
            self.assertAlmostEqual(actual, expected, delta=0.0001)


if __name__ == '__main__':
    unittest.main()
