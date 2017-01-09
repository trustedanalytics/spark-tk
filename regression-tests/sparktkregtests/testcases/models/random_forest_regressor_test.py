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

""" Tests the random forest functionality """

import unittest
from sparktkregtests.lib import sparktk_test


class RandomForest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForest, self).setUp()

        schema = [("feat1", int), ("feat2", int), ("class", float)]
        filename = self.get_file("rand_forest_class.csv")

        self.frame = self.context.frame.import_csv(filename, schema=schema)

    def test_train(self):
        """Test random forest train method"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, ["feat1", "feat2"], "class", seed=0)

        self.assertItemsEqual(model.observation_columns, ["feat1", "feat2"])
        self.assertEqual(model.label_column, "class")
        self.assertEqual(model.max_bins, 100)
        self.assertEqual(model.max_depth, 4)
        self.assertEqual(model.num_trees, 1)
        self.assertEqual(model.impurity, "variance")
        self.assertEqual(model.min_instances_per_node, 1)
        self.assertEqual(model.feature_subset_category, "auto")
        self.assertEqual(model.seed, 0)
        self.assertAlmostEqual(model.sub_sampling_rate, 1.0)
        self.assertIsNone(model.categorical_features_info)

    def test_predict(self):
        """Test predicted values are correct"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, ["feat1", "feat2"], "class", seed=0)

        result_frame = model.predict(self.frame)
        preddf = result_frame.to_pandas(self.frame.count())
        for index, row in preddf.iterrows():
            self.assertAlmostEqual(row['class'], row['predicted_value'])

    def test_negative_seed(self):
        """Test training with negative seed does not throw exception"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, ["feat1", "feat2"], "class", seed=-10)

    def test_bad_class_col_name(self):
        """Negative test to check behavior for bad class column"""
        with self.assertRaisesRegexp(
                Exception, "Invalid column name ERR provided"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "ERR")

    def test_bad_feature_col_name(self):
        """Negative test to check behavior for feature column"""
        with self.assertRaisesRegexp(
                Exception, ".*Invalid column name ERR provided"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["ERR", "feat2"], "class")

    def test_invalid_impurity(self):
        """Negative test for invalid impurity value"""
        with self.assertRaisesRegexp(
                Exception, "Only variance *"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class", impurity="gini")

    def test_negative_max_bins(self):
        """Negative test for max_bins < 0"""
        with self.assertRaisesRegexp(
                Exception, "Found max_bins = -1.  Expected non-negative integer."):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class", max_bins=-1)

    def test_max_bins_0(self):
        """Test for max_bins = 0; should not throw exception"""
        with self.assertRaisesRegexp(
                Exception,
                "maxBins must be greater than 0"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class", max_bins=0)

    def test_negative_max_depth(self):
        """Negative test for max_depth < 0"""
        with self.assertRaisesRegexp(
                Exception, "Found max_depth = -2.  Expected non-negative integer."):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class", max_depth=-2)

    def test_max_depth_0(self):
        """Negative test for max_depth=0"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, ["feat1", "feat2"], "class", max_depth=0)

        #check predicted values for depth 0
        result_frame = model.predict(self.frame)
        preddf = result_frame.to_pandas(self.frame.count())

        expected_pred_labels = [0.41299999999999998]*self.frame.count()
        actual_pred_labels = preddf['predicted_value'].tolist()

        self.assertItemsEqual(actual_pred_labels, expected_pred_labels)

    def test_negative_num_trees(self):
        """Negative test for num_trees<0"""
        with self.assertRaisesRegexp(
                Exception, "Found num_trees = -10.  Expected non-negative integer."):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class", num_trees=-10)

    def test_num_trees_0(self):
        """Negative test for num_trees=0"""
        with self.assertRaisesRegexp(
                Exception, "numTrees must be greater than 0"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class", num_trees=0)

    def test_invalid_feature_subset_category(self):
        """Negative test for feature subset category"""
        with self.assertRaisesRegexp(
                Exception, "feature subset category"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, ["feat1", "feat2"], "class",
                feature_subset_category="any")

    def test_rand_forest_save(self):
        """Test save plugin"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, ["feat1", "feat2"], "class", seed=0)
        path = self.get_name("test")
        model.save(path + "/randomforestregressor")
        restored = self.context.load(path +"/randomforestregressor")
        self.assertItemsEqual(restored.observation_columns, ["feat1", "feat2"])
        self.assertEqual(restored.label_column, "class")
        self.assertEqual(restored.max_bins, 100)
        self.assertEqual(restored.max_depth, 4)
        self.assertEqual(restored.num_trees, 1)
        self.assertEqual(restored.impurity, "variance")
        self.assertEqual(restored.min_instances_per_node, 1)
        self.assertEqual(restored.feature_subset_category, "auto")
        self.assertEqual(restored.seed, 0)
        self.assertAlmostEqual(restored.sub_sampling_rate, 1.0)
        self.assertIsNone(restored.categorical_features_info)

if __name__ == '__main__':
    unittest.main()
