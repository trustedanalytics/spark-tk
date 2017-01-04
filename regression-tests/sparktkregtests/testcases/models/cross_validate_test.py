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

import unittest
from sparktk.models import grid_values
from sparktkregtests.lib import sparktk_test

class CrossValidateTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(CrossValidateTest, self).setUp()
        binomial_dataset = self.get_file("small_logit_binary.csv")
        schema = [("vec0", float),
                  ("vec1", float),
                  ("vec2", float),
                  ("vec3", float),
                  ("vec4", float),
                  ("res", int),
                  ("count", int),
                  ("actual", int)]
        self.frame = self.context.frame.import_csv(
            binomial_dataset, schema=schema, header=True)

    def test_all_results(self):
        """Test number of models created given 5 folds """
        result = self.context.models.cross_validate(
            self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(5, 100),
            "step_size": 0.01}),
            (self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(2, 5, 15),
            "step_size": 0.001})],
            num_folds=5,
            verbose=False)

        #validate number of models
        all_models = result.all_results
        actual_num_models = 0
        svm_count = 0
        log_count = 0
        for fold in all_models:
            grid_points = fold.grid_points
            actual_num_models += len(grid_points)
            for grid_point in grid_points:
                if "svm" in grid_point.descriptor.model_type.__name__:
                    svm_count += 1
                else:
                    log_count += 1
        expected_num_models = 5 * (2 + 3)
        self.assertEquals(actual_num_models, expected_num_models)
        self.assertEqual(svm_count, 10)
        self.assertEqual(log_count, 15)

    def test_default_num_fold(self):
        """Test cross validate with default num_fold parameter"""
        result = self.context.models.cross_validate(
            self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(5, 100),
            "step_size": 0.01}),
            (self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(2, 5, 15),
            "step_size": 0.001})],
            verbose=False)

        #validate number of models
        all_models = result.all_results
        actual_num_models = 0
        svm_count = 0
        log_count = 0
        for fold in all_models:
            grid_points = fold.grid_points
            actual_num_models += len(grid_points)
            for grid_point in grid_points:
                if "svm" in grid_point.descriptor.model_type.__name__:
                    svm_count += 1
                else:
                    log_count += 1

        expected_num_models = 3 * (2 + 3)
        self.assertEquals(actual_num_models, expected_num_models)
        self.assertEqual(svm_count, 6)
        self.assertEqual(log_count, 9)


    def test_averages(self):
        """Test ouptut of cross validate averages"""
        result = self.context.models.cross_validate(
            self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(5, 100),
            "step_size": 0.01}),
            (self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(2, 15),
            "step_size": 0.001})],
            num_folds=3,
            verbose=False)

        avg_models = result.averages

        #validate num of models
        self.assertEqual(len(avg_models.grid_points), 4)

        #validate best model among all averages
        best_model = avg_models.find_best()
        self.assertEqual(
            best_model.descriptor.model_type.__name__,
            "sparktk.models.classification.logistic_regression")
        self.assertAlmostEqual(
            best_model.metrics.accuracy, .87, delta = 0.01)

    def test_invalid_num_fold(self):
        """Test cross validate with num_fold > number of data points"""
        with self.assertRaisesRegexp(
                Exception, "empty collection"):
            result = self.context.models.cross_validate(
                self.frame,
                [(self.context.models.classification.svm,
                {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(5, 100),
                "step_size": 0.01}),
                (self.context.models.classification.logistic_regression,
                {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
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
                self.frame,
                [(self.context.models.classification.svm,
                {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(5, 100),
                "step_size": 0.01}),
                (self.context.models.classification.logistic_regression,
                {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
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
                self.frame,
                [(self.context.models.classification.BAD,
                {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(5, 100),
                "step_size": 0.01}),
                (self.context.models.classification.logistic_regression,
                {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
                "label_column":"res",
                "num_iterations": grid_values(2, 15),
                "step_size": 0.001})],
                num_folds=2.5,
                verbose=False)

if __name__=="__main__":
    unittest.main()

