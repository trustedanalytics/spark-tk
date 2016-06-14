##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
""" usage: python2.7 linear_regression_test.py

Tests Linear Regression Model against known values.
"""

import unittest

import trustedanalytics as ia

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from qalib import frame_utils
from qalib import atk_test


class LinearRegression(atk_test.ATKTestCase):

    def setUp(self):
        """Build test frame"""
        super(LinearRegression, self).setUp()
        dataset = "linear_regression_gen.csv"
        schema = [("c1", ia.float64),
                  ("c2", ia.float64),
                  ("c3", ia.float64),
                  ("c4", ia.float64),
                  ("label", ia.float64)]

        self.frame = frame_utils.build_frame(
            dataset, schema, self.prefix)

    def test_model_publish(self):
        """Test publishing a linear regression model"""
        model = ia.LinearRegressionModel()
        model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'])
        model_path = model.publish()
        self.assertIn("hdfs", model_path)
        self.assertIn("tar", model_path)

    def test_model_test(self):
        """Test test functionality"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'])
        output = model.test(self.frame, "label")
        self.assertAlmostEqual(
            res['mean_squared_error'], output['mean_squared_error'])
        self.assertAlmostEqual(
            res['root_mean_squared_error'], output['root_mean_squared_error'])
        self.assertAlmostEqual(
            res['mean_absolute_error'], output['mean_absolute_error'])
        self.assertAlmostEqual(
            res['explained_variance'], output['explained_variance'])

    def test_model_predict_output(self):
        """Test output format of predict"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'])
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def test_model_elastic_net(self):
        """Test elastic net argument"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          elastic_net_parameter=0.3)
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def test_model_fix_intercept(self):
        """Test fix intercept argument"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          fit_intercept=False)
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def test_model_max_iterations(self):
        """Test max iterations argument"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          max_iterations=70)
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def test_model_reg_param(self):
        """Test regularization parameter argument"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          reg_param=0.000000002)
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def test_model_standardization(self):
        """Test test non-standardized data"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          standardization=False)
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def test_model_tolerance(self):
        """Test test a different model tolerance"""
        model = ia.LinearRegressionModel()
        res = model.train(self.frame, "label", ['c1', 'c2', 'c3', 'c4'],
                          tolerance=0.0000000000000000001)
        predict = model.predict(self.frame)
        self._validate_results(res, predict)

    def _validate_results(self, res, predict):
        # validate dictionary entries, weights, and predict results
        self.assertAlmostEqual(res["mean_absolute_error"], 0.0)
        self.assertAlmostEqual(res["root_mean_squared_error"], 0.0)
        self.assertAlmostEqual(res["mean_squared_error"], 0.0)
        self.assertAlmostEqual(res["intercept"], 0.0)
        self.assertIn("objective_history", res)
        self.assertIn("explained_variance", res)
        self.assertEqual(res["value_column"], "label")
        self.assertItemsEqual(
            res["observation_columns"], ['c1', 'c2', 'c3', 'c4'])
        self.assertLess(res["iterations"], 150)

        for (i, j) in zip([0.5, -0.7, -0.24, 0.4], res['weights']):
            self.assertAlmostEqual(i, j, places=4)

        pd_res = predict.download(predict.row_count)
        for _, i in pd_res.iterrows():
            self.assertAlmostEqual(i["label"], i["predicted_value"], places=4)


if __name__ == '__main__':
    unittest.main()
