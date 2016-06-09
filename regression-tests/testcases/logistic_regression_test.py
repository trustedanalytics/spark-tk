##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
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
"""
 Tests Logistic Regression Model against known values, calculated in R.
"""

__author__ = "lcoatesx"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class LogisticRegression(atk_test.ATKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(LogisticRegression, self).setUp()

        binomial_dataset = "small_logit_binary.csv"

        schema = [("vec0", ia.float64),
                  ("vec1", ia.float64),
                  ("vec2", ia.float64),
                  ("vec3", ia.float64),
                  ("vec4", ia.float64),
                  ("res", ia.int32),
                  ("count", ia.int32),
                  ("actual", ia.int32)]
        self.binomial_frame = frame_utils.build_frame(
            binomial_dataset, schema, self.prefix, skip_header_lines=1)

        self.schema = schema

    def test_predict(self):
        """test predict works as expected"""
        log_model = ia.LogisticRegressionModel()
        log_model.train(
            self.binomial_frame,
            "res", ["vec0", "vec1", "vec2", "vec3", "vec4"])
        values = log_model.predict(self.binomial_frame)
        frame = values.download(values.row_count)
        for _, row in frame.iterrows():
            self.assertEqual(row["actual"], row["predicted_label"])

    def test_bad_predict_frame_type(self):
        """test invalid frame on predict"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"])
            log_model.predict(7)

    def test_bad_predict_observation(self):
        """test invalid observations on predict"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"])
            log_model.predict(self.binomial_frame, 7)

    def test_bad_predict_observation_value(self):
        """test invalid observation value on predict"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"])
            log_model.predict(self.binomial_frame, ["err"])

    def test_bad_feature_column_type(self):
        """test invalid feature column type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.test(
                self.binomial_frame, "res", 7)

    def test_no_feature_column(self):
        """test invalid feature column name"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.test(
                self.binomial_frame,
                "ERR", ["vec0", "vec1", "blah", "vec3", "vec4"])

    def test_no_feature_column_train(self):
        """test invalid feature column name in train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "ERR", ["vec0", "vec1", "blah", "vec3", "vec4"])

    def test_feature_column_type_train(self):
        """test invalid feature column type name in train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                7, ["vec0", "vec1", "blah", "vec3", "vec4"])

    def test_observation_column_invalid(self):
        """test invalid observation column name in train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["err", "vec1", "blah", "vec3", "vec4"])

    def test_count_column_type(self):
        """test invalid count type in train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "blah", "vec3", "vec4"], 7)

    def test_bad_optimizer_type(self):
        """test invalid optimizer type train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "blah", "vec3", "vec4"], optimizer=7)

    def test_bad_optimizer_value(self):
        """test invalid optmizer value train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "blah", "vec3", "vec4"],
                optimizer="err")

    def test_count_column_value(self):
        """test invalid count value train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "blah", "vec3", "vec4"], "err")

    def test_type_num_classes(self):
        """test num classes invalid typetrain"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "blah", "vec3", "vec4"],
                num_classes="err")

    def test_observation_column_type(self):
        """test invalid observation column type train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(self.binomial_frame, "res", 7)

    def test_frame_type(self):
        """test invalid frame type train"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                "ERR", "res", ["vec0", "vec1", "blah", "vec3", "vec4"])

    def test_logistic_regression_bad_frame(self):
        """test invalid frame"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.test(
                "ERR", "res", ["vec0", "vec1", "vec2", "vec3", "vec4"])

    def test_optimizer_bad_type(self):
        """test optimizer type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"], optimizer=7)

    def test_optimizer_bad_value(self):
        """test invalid optimizer value"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                optimizer="ERR")

    def test_optimizer_bad_config(self):
        """test invalid sgd configuraton"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                optimizer="SGD", num_classes=4)

    def test_covariance_type(self):
        """test invalid covariance type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                compute_covariance=7)

    def test_covariance_false(self):
        """test not computing covariance"""
        log_model = ia.LogisticRegressionModel()
        values = log_model.train(
            self.binomial_frame,
            "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
            compute_covariance=False)
        self.assertIsNone(values.covariance_matrix)

    def test_intercept_false(self):
        """test not having an intercept"""
        log_model = ia.LogisticRegressionModel()
        values = log_model.train(
            self.binomial_frame,
            "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
            intercept=False)
        self.assertNotIn("intercept", values.summary_table.coefficients.keys())

    def test_intercept_type(self):
        """test invalid intercept type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                intercept=7)

    def test_feature_scaling_type(self):
        """test feature scaling type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                feature_scaling=7)

    def test_num_corrections_type(self):
        """test invalid num corrections type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                num_corrections="ERR")

    def test_threshold_type(self):
        """test invalid threshold type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                threshold="ERR")

    def test_regularization_invalid(self):
        """test regularization parameter"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                reg_type="ERR")

    def test_num_iterations_type(self):
        """test invalid num iterations type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                num_iterations="ERR")

    def test_regularization_value_type(self):
        """test invalid regularization value"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                reg_param="ERR")

    def test_convergence_type(self):
        """test invalid convergence type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                convergence_tolerance="ERR")

    def test_regularization_type(self):
        """test invalid reg type type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                reg_type=7)

    def test_step_size_type(self):
        """test invalid step size type"""
        with self.assertRaises(ia.rest.command.CommandServerError):
            log_model = ia.LogisticRegressionModel()
            log_model.train(
                self.binomial_frame,
                "res", ["vec0", "vec1", "vec2", "vec3", "vec4"],
                step_size="ERR")

    def test_logistic_regression_test(self):
        """test the default happy path of logistic regression test"""
        log_model = ia.LogisticRegressionModel()
        log_model.train(
            self.binomial_frame, "res",
            ["vec0", "vec1", "vec2", "vec3", "vec4"])
        values = log_model.test(
            self.binomial_frame, "res",
            ["vec0", "vec1", "vec2", "vec3", "vec4"])

        print values
        tp_f = self.binomial_frame.copy()
        tp_f.filter(lambda x: x['res'] == 1 and x['actual'] == 1)
        tp = float(tp_f.row_count)

        tn_f = self.binomial_frame.copy()
        tn_f.filter(lambda x: x['res'] == 0 and x['actual'] == 0)
        tn = float(tn_f.row_count)

        fp_f = self.binomial_frame.copy()
        fp_f.filter(lambda x: x['res'] == 0 and x['actual'] == 1)
        fp = float(fp_f.row_count)

        fn_f = self.binomial_frame.copy()
        fn_f.filter(lambda x: x['res'] == 1 and x['actual'] == 0)
        fn = float(fn_f.row_count)

        # manually build the confusion matrix and derivitive properties
        print "tn", tn
        print "fp", fp
        print "tp", tp
        print "fn", fn
        precision = tp/(tp+fp)
        recall = tp/(tp+fn)

        self.assertAlmostEqual(values.precision, precision)
        self.assertAlmostEqual(values.recall, recall)
        self.assertAlmostEqual(
            values.f_measure, 2*(precision*recall)/(precision+recall))
        self.assertAlmostEqual(values.accuracy, (tp+tn)/(tp+fn+tn+fp))

        self.assertAlmostEqual(
            values.confusion_matrix["Predicted_Pos"]["Actual_Pos"], tp)
        self.assertAlmostEqual(
            values.confusion_matrix["Predicted_Pos"]["Actual_Neg"], fp)
        self.assertAlmostEqual(
            values.confusion_matrix["Predicted_Neg"]["Actual_Neg"], tn)
        self.assertAlmostEqual(
            values.confusion_matrix["Predicted_Neg"]["Actual_Pos"], fn)

    def test_logistic_regression_train(self):
        """test logistic regression train"""
        log_model = ia.LogisticRegressionModel()
        summary = log_model.train(
            self.binomial_frame, "res",
            ["vec0", "vec1", "vec2", "vec3", "vec4"])
        print summary
        self._standard_summary(summary, False)

    def test_logistic_regression_train_sgd(self):
        """test logistic regression train with sgd"""
        log_model = ia.LogisticRegressionModel()
        summary = log_model.train(
            self.binomial_frame, "res",
            ["vec0", "vec1", "vec2", "vec3", "vec4"], optimizer="SGD",
            num_iterations=250, step_size=12)
        print summary
        self._standard_summary(summary, False)

    def test_logistic_regression_train_count_column(self):
        """test logistic regression train with count"""
        log_model = ia.LogisticRegressionModel()
        summary = log_model.train(
            self.binomial_frame, "res",
            ["vec0", "vec1", "vec2", "vec3", "vec4"], "count")
        print summary
        self._standard_summary(summary, True)

    def _standard_summary(self, summary, coefficients_only):
        """verfies the summary object against calculated values from R"""
        self.assertEqual(summary.num_features, 5)
        self.assertEqual(summary.num_classes, 2)

        self.assertAlmostEqual(
            summary.summary_table["coefficients"]["intercept"],
            0.9, delta=0.01)
        self.assertAlmostEqual(
            summary.summary_table["coefficients"]["vec0"], 0.4, delta=0.01)
        self.assertAlmostEqual(
            summary.summary_table["coefficients"]["vec1"], 0.7, delta=0.01)
        self.assertAlmostEqual(
            summary.summary_table["coefficients"]["vec2"], 0.9, delta=0.01)
        self.assertAlmostEqual(
            summary.summary_table["coefficients"]["vec3"], 0.3, delta=0.01)
        self.assertAlmostEqual(
            summary.summary_table["coefficients"]["vec4"], 1.4, delta=0.01)

        if not coefficients_only:
            self.assertAlmostEqual(
                summary.summary_table["degrees_freedom"]["intercept"], 1)
            self.assertAlmostEqual(
                summary.summary_table["standard_errors"]["intercept"],
                0.012222, delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["wald_statistic"]["intercept"], 73.62,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["p_value"]["intercept"], 0,
                delta=0.01)

            self.assertAlmostEqual(
                summary.summary_table["degrees_freedom"]["vec0"], 1)
            self.assertAlmostEqual(
                summary.summary_table["standard_errors"]["vec0"], 0.005307,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["wald_statistic"]["vec0"], 75.451032,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["p_value"]["vec0"], 0,
                delta=0.01)

            self.assertAlmostEqual(
                summary.summary_table["degrees_freedom"]["vec1"], 1)
            self.assertAlmostEqual(
                summary.summary_table["standard_errors"]["vec1"], 0.006273,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["wald_statistic"]["vec1"], 110.938600,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["p_value"]["vec1"], 0,
                delta=0.01)

            self.assertAlmostEqual(
                summary.summary_table["degrees_freedom"]["vec2"], 1)
            self.assertAlmostEqual(
                summary.summary_table["standard_errors"]["vec2"], 0.007284,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["wald_statistic"]["vec2"], 124.156836,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["p_value"]["vec2"], 0,
                delta=0.01)

            self.assertAlmostEqual(
                summary.summary_table["degrees_freedom"]["vec3"], 1)
            self.assertAlmostEqual(
                summary.summary_table["standard_errors"]["vec3"], 0.005096,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["wald_statistic"]["vec3"], 58.617307,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["p_value"]["vec3"], 0,
                delta=0.01)

            self.assertAlmostEqual(
                summary.summary_table["degrees_freedom"]["vec4"], 1)
            self.assertAlmostEqual(
                summary.summary_table["standard_errors"]["vec4"], 0.009784,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["wald_statistic"]["vec4"], 143.976980,
                delta=0.01)
            self.assertAlmostEqual(
                summary.summary_table["p_value"]["vec4"], 0,
                delta=0.01)

            r_cov = [
                [1.495461e-04, 1.460983e-05, 2.630870e-05,
                 3.369595e-05, 9.938721e-06, 5.580564e-05],
                [1.460983e-05, 2.816412e-05, 1.180398e-05,
                 1.477251e-05, 6.008541e-06, 2.206105e-05],
                [2.630870e-05, 1.180398e-05, 3.935554e-05,
                 2.491004e-05, 8.871815e-06, 4.022959e-05],
                [3.369595e-05, 1.477251e-05, 2.491004e-05,
                 5.305153e-05, 1.069435e-05, 5.243129e-05],
                [9.938721e-06, 6.008541e-06, 8.871815e-06,
                 1.069435e-05, 2.596849e-05, 1.776645e-05],
                [5.580564e-05, 2.206105e-05, 4.022959e-05,
                 5.243129e-05, 1.776645e-05, 9.572358e-05]]

            summ_cov = summary.covariance_matrix.take(
                summary.covariance_matrix.row_count)

            for (r_list, summ_list) in zip(r_cov, summ_cov):
                for (r_val, summ_val) in zip(r_list, summ_list):
                    self.assertAlmostEqual(r_val, summ_val)


if __name__ == '__main__':
    unittest.main()
