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
            self.frame, "class", ["feat1", "feat2"], seed=0)
        
        self.assertEqual(model.max_bins, 100)
        self.assertEqual(model.max_depth, 4)
        self.assertEqual(model.num_trees, 1)
        self.assertEqual(model.impurity, 'variance')

    def test_predict(self):
        """Test predicted values are correct"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)

        model.predict(self.frame)
        preddf = self.frame.to_pandas(self.frame.count())
        for index, row in preddf.iterrows():
            self.assertAlmostEqual(row['class'], row['predicted_value'])

    def test_negative_seed(self):
        """Test training with negative seed does not throw exception"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=-10)

    def test_bad_class_col_name(self):
        """Negative test to check behavior for bad class column"""
        with self.assertRaisesRegexp(
                Exception, "Invalid column name ERR provided"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "ERR", ["feat1", "feat2"])

    def test_bad_feature_col_name(self):
        """Negative test to check behavior for feature column"""
        with self.assertRaisesRegexp(
                Exception, ".*Invalid column name ERR provided"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["ERR", "feat2"])

    def test_invalid_impurity(self):
        """Negative test for invalid impurity value"""
        with self.assertRaisesRegexp(
                Exception, "Only variance *"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"], impurity="gini")

    def test_negative_max_bins(self):
        """Negative test for max_bins < 0"""
        with self.assertRaisesRegexp(
                Exception, ".*invalid maxBins parameter.*"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"], max_bins=-1)

    def test_max_bins_0(self):
        """Test for max_bins = 0; should not throw exception"""
        with self.assertRaisesRegexp(
                Exception,
                "DecisionTree Strategy given invalid maxBins parameter"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"], max_bins=0)

    def test_negative_max_depth(self):
        """Negative test for max_depth < 0"""
        with self.assertRaisesRegexp(
                Exception, "maxDepth must be non negative"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"], max_depth=-2)

    def test_max_depth_0(self):
        """Negative test for max_depth=0"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], max_depth=0)

        #check predicted values for depth 0
        model.predict(self.frame)
        preddf = self.frame.to_pandas(self.frame.count())

        expected_pred_labels = [0.41299999999999998]*self.frame.count()
        actual_pred_labels = preddf['predicted_value'].tolist()

        self.assertItemsEqual(actual_pred_labels, expected_pred_labels)

    def test_negative_num_trees(self):
        """Negative test for num_trees<0"""
        with self.assertRaisesRegexp(
                Exception, "numTrees must be greater than 0"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"], num_trees=-10)

    def test_num_trees_0(self):
        """Negative test for num_trees=0"""
        with self.assertRaisesRegexp(
                Exception, "numTrees must be greater than 0"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"], num_trees=0)

    def test_invalid_feature_subset_category(self):
        """Negative test for feature subset category"""
        with self.assertRaisesRegexp(
                Exception, "feature subset category"):
            model = self.context.models.regression.random_forest_regressor.train(
                self.frame, "class", ["feat1", "feat2"],
                feature_subset_category="any")

    def test_rand_forest_publish(self):
        """Test binomial classification of random forest model"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        model.save("test/randomforestregressor")
        restored = self.context.load("test/randomforestregressor")
        self.assertEqual(restored.max_bins, 100)
        self.assertEqual(restored.max_depth, 4)
        self.assertEqual(restored.num_trees, 1)
        self.assertEqual(restored.impurity, 'variance')

if __name__ == '__main__':
    unittest.main()
