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

"""Testcases for cross_validate module in Best Parameter and Model Selection"""

import unittest
from sparktk.models import grid_values
from sparktkregtests.lib import sparktk_test


class CrossValidateTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(CrossValidateTest, self).setUp()
        classifier_dataset = self.get_file("small_logit_binary.csv")
        schema1 = [
                    ("vec0", float),
                    ("vec1", float),
                    ("vec2", float),
                    ("vec3", float),
                    ("vec4", float),
                    ("res", int),
                    ("count", int),
                    ("actual", int)]
        self.classifier_frame = self.context.frame.import_csv(
            classifier_dataset, schema=schema1, header=True)

        schema2 = [("feat1", int), ("feat2", int), ("class", float)]
        regressor_dataset = self.get_file("rand_forest_class.csv")
        self.regressor_frame = self.context.frame.import_csv(
            regressor_dataset, schema=schema2)

    def test_all_results_classifiers(self):
        """Test number of classifiers created given 5 folds """
        result = self.context.models.cross_validate(
            self.classifier_frame,
            [(
             self.context.models.classification.svm,
             {
                "observation_columns":
                ["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(5, 100),
                "step_size": 0.01}),
             (
             self.context.models.classification.logistic_regression,
             {
                "observation_columns":
                ["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(2, 5, 15),
                "step_size": 0.001})],
            num_folds=5,
            verbose=False)

        # validate number of models
        (svm_count, log_count, num_models) = self._get_model_counts(
                result, "svm")
        expected_num_models = 5 * (2 + 3)
        self.assertEquals(num_models, expected_num_models)
        self.assertEqual(svm_count, 10)
        self.assertEqual(log_count, 15)

    def test_all_results_regressors(self):
        """Test number of regressors created given 5 folds """
        result = self.context.models.cross_validate(
            self.regressor_frame,
            [(
             self.context.models.regression.linear_regression,
             {
                "observation_columns": ["feat1", "feat2"],
                "label_column":"class",
                "max_iterations": grid_values(*xrange(5, 10)),
                "elastic_net_parameter": 0.001}),
             (
             self.context.models.regression.random_forest_regressor,
             {
                "observation_columns": ["feat1", "feat2"],
                "label_column":"class",
                "num_trees": grid_values(2, 5, 15),
                "max_depth": 5})],
            num_folds=5,
            verbose=False)

        # validate number of models
        (rf_count, linreg_count, num_models) = self._get_model_counts(
            result, "random_forest")
        expected_num_models = 5 * (5 + 3)
        self.assertEquals(rf_count + linreg_count, expected_num_models)
        self.assertEqual(rf_count, 15)
        self.assertEqual(linreg_count, 25)

    def test_default_num_fold(self):
        """Test cross validate with default num_fold parameter"""
        result = self.context.models.cross_validate(
            self.classifier_frame,
            [(
             self.context.models.classification.svm,
             {
                "observation_columns":
                ["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(5, 100),
                "step_size": 0.01}),
             (
             self.context.models.classification.logistic_regression,
             {
                "observation_columns":
                ["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column": "res",
                "num_iterations": grid_values(2, 5, 15),
                "step_size": 0.001})],
            verbose=False)

        # validate number of models
        (svm_count, log_count, num_models) = self._get_model_counts(
                result, "svm")
        expected_num_models = 3 * (2 + 3)
        self.assertEquals(num_models, expected_num_models)
        self.assertEqual(svm_count, 6)
        self.assertEqual(log_count, 9)

    def test_single_fold(self):
        """Test cross validate with num_folds = 1; should throw exception"""
        with self.assertRaises(
                Exception):
            self.context.models.cross_validate(
                self.regressor_frame,
                [(
                self.context.models.regression.linear_regression,
                {
                    "observation_columns":
                    ["feat1", "feat2"],
                    "label_column": "class",
                    "max_iterations": grid_values(5, 100),
                    "reg_param": 0.0001}),
                (
                self.context.models.regression.random_forest_regressor,
                {
                    "observation_columns":
                    ["feat1", "feat2"],
                    "label_column": "class",
                    "num_trees": grid_values(2, 5, 8),
                    "max_depth": 5})],
                verbose=False,
                num_folds=1)

    def test_two_folds(self):
        """Test cross validate with num_folds = 2"""
        result = self.context.models.cross_validate(
            self.regressor_frame,
            [(
             self.context.models.regression.linear_regression,
             {
                "observation_columns":
                ["feat1", "feat2"],
                "label_column": "class",
                "max_iterations": grid_values(5, 100),
                "reg_param": 0.0001}),
             (
             self.context.models.regression.random_forest_regressor,
             {
                "observation_columns":
                ["feat1", "feat2"],
                "label_column": "class",
                "num_trees": grid_values(2, 5, 8),
                "max_depth": 5})],
            verbose=False,
            num_folds=2)

        # validate number of models
        (rf_count, linreg_count, num_models) = self._get_model_counts(
                result, "random")
        expected_num_models = 2 * (2 + 3)
        self.assertEquals(num_models, expected_num_models)
        self.assertEqual(rf_count, 6)
        self.assertEqual(linreg_count, 4)

    def test_averages_classifiers(self):
        """Test ouptut of cross validatation averages for classifiers"""
        result = self.context.models.cross_validate(
            self.classifier_frame,
            [(
             self.context.models.classification.svm,
             {
                "observation_columns":
                ["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(5, 100),
                "step_size": 0.01}),
             (
             self.context.models.classification.logistic_regression,
             {
                "observation_columns":
                ["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(2, 15),
                "step_size": 0.001})],
            num_folds=3,
            verbose=False)

        avg_models = result.averages

        # validate num of models
        self.assertEqual(len(avg_models.grid_points), 4)

        # validate model with best accuracy
        best_model = avg_models.find_best()
        self.assertEqual(
            best_model.descriptor.model_type.__name__,
            "sparktk.models.classification.logistic_regression")
        self.assertAlmostEqual(
            best_model.metrics.accuracy, .87, delta=0.01)

    def test_averages_regressors(self):
        """Test ouptut of cross validatation averages for regressors"""
        result = self.context.models.cross_validate(
            self.regressor_frame,
            [(
             self.context.models.regression.linear_regression,
             {
                "observation_columns":
                ["feat1", "feat2"],
                "label_column":"class",
                "max_iterations": grid_values(*xrange(10, 20)),
                "reg_param": 0.001}),
             (
             self.context.models.regression.random_forest_regressor,
             {
                "observation_columns":
                ["feat1", "feat2"],
                "label_column":"class",
                "num_trees": grid_values(*xrange(2, 5)),
                "max_depth": 4})],
            num_folds=3,
            verbose=False)

        avg_models = result.averages

        # validate num of models
        self.assertEqual(len(avg_models.grid_points), 13)

        # validate model with best accuracy
        best_model = avg_models.find_best()
        self.assertEqual(
            best_model.descriptor.model_type.__name__,
            "sparktk.models.regression.random_forest_regressor")
        self.assertAlmostEqual(
            best_model.metrics.r2, 0.415, delta=0.01)

    def test_invalid_num_fold(self):
        """Test cross validate with num_fold > number of data points"""
        with self.assertRaisesRegexp(
                Exception, "empty collection"):
            result = self.context.models.cross_validate(
                self.classifier_frame,
                [(
                 self.context.models.classification.svm,
                 {
                    "observation_columns":
                    ["vec0", "vec1", "vec2", "vec3", "vec4"],
                    "label_column":"res",
                    "num_iterations": grid_values(5, 100),
                    "step_size": 0.01}),
                 (
                 self.context.models.classification.logistic_regression,
                 {
                    "observation_columns":
                    ["vec0", "vec1", "vec2", "vec3", "vec4"],
                    "label_column":"res",
                    "num_iterations": grid_values(2, 15),
                    "step_size": 0.001})],
                num_folds=1000000,
                verbose=False)

    def test_float_num_fold(self):
        """Test cross validate with float num_fold"""
        with self.assertRaisesRegexp(
                Exception, "integer argument expected, got float"):
            result = self.context.models.cross_validate(
                self.classifier_frame,
                [(
                 self.context.models.classification.svm,
                 {
                    "observation_columns":
                    ["vec0", "vec1", "vec2", "vec3", "vec4"],
                    "label_column":"res",
                    "num_iterations": grid_values(5, 100),
                    "step_size": 0.01}),
                 (
                 self.context.models.classification.logistic_regression,
                 {
                    "observation_columns":
                    ["vec0", "vec1", "vec2", "vec3", "vec4"],
                    "label_column":"res",
                    "num_iterations": grid_values(2, 15),
                    "step_size": 0.001})],
                num_folds=2.5,
                verbose=False)

    def test_invalid_model(self):
        """Test cross validate with invalid model"""
        with self.assertRaisesRegexp(
                Exception, "no attribute \'BAD\'"):
            result = self.context.models.cross_validate(
                self.classifier_frame,
                [(
                 self.context.models.classification.BAD,
                 {
                    "observation_columns":
                    ["vec0", "vec1", "vec2", "vec3", "vec4"],
                    "label_column":"res",
                    "num_iterations": grid_values(5, 100),
                    "step_size": 0.01}),
                 (
                 self.context.models.classification.logistic_regression,
                 {
                    "observation_columns":
                    ["vec0", "vec1", "vec2", "vec3", "vec4"],
                    "label_column":"res",
                    "num_iterations": grid_values(2, 15),
                    "step_size": 0.001})],
                num_folds=2.5,
                verbose=False)

    def _get_model_counts(self, result, model_name):
        # validate number of models
        all_models = result.all_results
        model1_count = 0
        model2_count = 0
        num_models = 0
        for fold in all_models:
            grid_points = fold.grid_points
            num_models += len(grid_points)
            for grid_point in grid_points:
                if model_name in grid_point.descriptor.model_type.__name__:
                    model1_count += 1
                else:
                    model2_count += 1
        return (model1_count, model2_count, num_models)
if __name__ == "__main__":
    unittest.main()
