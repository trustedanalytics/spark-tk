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
from sparktk.models import grid_values
from sparktkregtests.lib import sparktk_test

class GridSearchTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(GridSearchTest, self).setUp()
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

    def test_grid_points(self):
        """Test output of grid search on svm and logistic regression"""
        grid_result = self.context.models.grid_search(
            self.frame, self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(5, 100),
            "step_size": 0.01}),
            (self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(2, 15),
            "step_size": 0.001})])

        grid_points = grid_result.grid_points

        #validate number of items in grid
        self.assertEqual(len(grid_points), 4)

        #validate one of the models' name
        self.assertEqual(
            grid_points[0].descriptor.model_type.__name__,
            "sparktk.models.classification.svm")
      
        #validate grid values of the first model
        svm_kwargs_0 = grid_points[0].descriptor.kwargs
        self.assertEqual(svm_kwargs_0['num_iterations'], 5)
        self.assertEqual(svm_kwargs_0['step_size'], 0.01)
        self.assertEqual(svm_kwargs_0['label_column'], "res")
        self.assertItemsEqual(svm_kwargs_0['observation_columns'], ["vec0", "vec1", "vec2", "vec3", "vec4"])

        #validate grid values of the second model
        svm_kwargs_1 = grid_points[1].descriptor.kwargs
        self.assertEqual(svm_kwargs_1['num_iterations'], 100)
        self.assertEqual(svm_kwargs_1['step_size'], 0.01)
        self.assertEqual(svm_kwargs_1['label_column'], "res")
        self.assertItemsEqual(svm_kwargs_1['observation_columns'], ["vec0", "vec1", "vec2", "vec3", "vec4"])

        #validate grid values of the third model
        lr_kwargs_0 = grid_points[2].descriptor.kwargs
        self.assertEqual(lr_kwargs_0['num_iterations'], 2)
        self.assertEqual(lr_kwargs_0['step_size'], 0.001)
        self.assertEqual(lr_kwargs_0['label_column'], "res")
        self.assertItemsEqual(lr_kwargs_0['observation_columns'], ["vec0", "vec1", "vec2", "vec3", "vec4"])

        #validate grid values of the third model
        lr_kwargs_1 = grid_points[3].descriptor.kwargs
        self.assertEqual(lr_kwargs_1['num_iterations'], 15)
        self.assertEqual(lr_kwargs_1['step_size'], 0.001)
        self.assertEqual(lr_kwargs_1['label_column'], "res")
        self.assertItemsEqual(lr_kwargs_1['observation_columns'], ["vec0", "vec1", "vec2", "vec3", "vec4"])

	#validate accuracy metric of one of the models
	self.assertEquals(grid_points[2].metrics.accuracy, 0.8745)

    def test_find_best(self):
        """Test find best in grid_search"""
        grid_result = self.context.models.grid_search(
            self.frame, self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(5, 10),
            "step_size": 0.01}),
            (self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(*xrange(2, 15)),
            "step_size": 0.001})])

        best_model = grid_result.find_best()
        self.assertEqual(
	    best_model.descriptor.model_type.__name__,
            "sparktk.models.classification.logistic_regression")
        self.assertAlmostEqual(best_model.metrics.accuracy, 0.87688, delta=0.01)

    def test_grid_values_with_xrange(self):
        """Test grid values with xrange"""
        grid_result = self.context.models.grid_search(
            self.frame, self.frame,
            [(self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(*xrange(5, 10)),
            "step_size": 0.001})])

        #validate number of models in the grid
        self.assertEquals(len(grid_result.grid_points), 5)

    def test_empty_tuple_list(self):
        """Test empty list of tuples throws exception"""
        with self.assertRaisesRegexp(
                Exception, "tuple index out of range"):
            self.context.models.grid_search(
                self.frame, self.frame,
                [()])

    def test_incorect_hyper_parameter(self):
        """Test incorrect hyper parameter name for a model throws exception"""
        with self.assertRaisesRegexp(
                Exception, "unknown args named: BAD"):
            self.context.models.grid_search(
            self.frame, self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "BAD":"res",
            "num_iterations": grid_values(5, 100),
            "step_size": 0.01}),
            (self.context.models.classification.logistic_regression,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "BAD":"res",
            "num_iterations": grid_values(2, 15),
            "step_size": 0.001})])

    def test_bad_data_type_in_grid_values(self):
        """Test invalid parameter to grid_values throws exception"""
        with self.assertRaisesRegexp(
                Exception, "Method .* does not exist"):
            self.context.models.grid_search(
            self.frame, self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values("one"),
            "step_size": 0.001})])

    def test_missing_test_frame(self):
        """Test grid search throws exception for missing test frame"""
        with self.assertRaisesRegexp(
                Exception, "takes at least 3 arguments"):
            self.context.models.grid_search(
            self.frame,
            [(self.context.models.classification.svm,
            {"observation_columns":["vec0", "vec1", "vec2", "vec3", "vec4"],
            "label_column":"res",
            "num_iterations": grid_values(1, 4),
            "step_size": 0.001})])

if __name__=="__main__":
    unittest.main()

