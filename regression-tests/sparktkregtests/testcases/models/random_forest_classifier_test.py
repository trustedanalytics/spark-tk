""" Tests the random forest functionality """
import unittest
from sparktkregtests.lib import sparktk_test


class RandomForest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForest, self).setUp()

        schema = [("feat1", int), ("feat2", int), ("class", str)]
        filename = self.get_file("rand_forest_class.csv")

        self.frame = self.context.frame.import_csv(filename, schema=schema)

    def test_classifier_train(self):
        """ Test train method """
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        self.assertEqual(model.max_bins, 100)
        self.assertEqual(model.max_depth, 4)
        self.assertEqual(model.num_classes, 2)
        self.assertEqual(model.num_trees, 1)
        self.assertEqual(model.impurity, 'gini')

    def test_classifier_predict(self):
        """Test binomial classification of random forest model"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)

        model.predict(self.frame)
        preddf = self.frame.to_pandas(self.frame.count())
        for index, row in preddf.iterrows():
            self.assertEqual(row['class'], str(row['predicted_class']))

    def test_classifier_test(self):
        """Test test() method"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        test_res = model.test(self.frame)

        self.assertEqual(test_res.precision, 1.0)
        self.assertEqual(test_res.recall, 1.0)
        self.assertEqual(test_res.accuracy, 1.0)
        self.assertEqual(test_res.f_measure, 1.0)

        self.assertEqual(
            test_res.confusion_matrix['Predicted_Pos']['Actual_Pos'], 413)
        self.assertEqual(
            test_res.confusion_matrix['Predicted_Pos']['Actual_Neg'], 0)
        self.assertEqual(
            test_res.confusion_matrix['Predicted_Neg']['Actual_Pos'], 0)
        self.assertEqual(
            test_res.confusion_matrix['Predicted_Neg']['Actual_Neg'], 587)

    def test_negative_seed(self):
        """Test training with negative seed does not throw exception"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], seed=-10)

    def test_bad_class_col_name(self):
        """Negative test to check behavior for bad class column"""
        with self.assertRaisesRegexp(
                Exception, ".*Invalid column name ERR provided .*"):
            model = self.context.models.classification.random_forest_classifier.train(
                self.frame, "ERR", ["feat1", "feat2"])

    def test_bad_feature_col_name(self):
        """Negative test to check behavior for feature class column"""
        with self.assertRaisesRegexp(
                Exception, ".*Invalid column name ERR provided"):
            model = self.context.models.classification.random_forest_classifier.train(
                self.frame, "class", ["ERR", "feat2"])

    def test_invalid_impurity(self):
        """Negative test for invalid impurity value"""
        model = self.context.models.classification.random_forest_classifier.train(           
            self.frame, "class", ["feat1", "feat2"], impurity="variance")

    def test_negative_max_bins(self):
        """Negative test for max_bins < 0"""
        with self.assertRaisesRegexp(
                Exception, ".*invalid maxBins parameter.*"):
            model = self.context.models.classification.random_forest_classifier.train(
                self.frame, "class", ["feat1", "feat2"], max_bins=-1)

    def test_max_bins_0(self):
        """Test for max_bins = 0; should not throw exception"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], max_bins=0)
        #to-do: check predicted values for depth 0:

    def test_negative_max_depth(self):
        """Negative test for max_depth < 0"""
        with self.assertRaisesRegexp(
                Exception, "maxDepth must be non negative"):
            model = self.context.models.classification.random_forest_classifier.train(
                self.frame, "class", ["feat1", "feat2"], max_depth=-2)

    def test_max_depth_0(self):
        """Negative test for max_depth=0"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], max_depth=0)
        #to-do: check predicted values for depth 0:


    def test_negative_num_trees(self):
        """Negative test for num_trees<0"""
        with self.RaisesRegexp(
                Exception, "numTrees must be greater than 0"):
            model = self.context.models.classification.random_forest_classifier.train(
                self.frame, "class", ["feat1", "feat2"], num_trees=-10)

    def test_num_trees_0(self):
        """Negative test for num_trees=0"""
        with self.assertRaisesRegexp(
                Exception, "numTrees must be greater than 0"):
            model = self.context.models.classification.random_forest_classifier.train(
                self.frame, "class", ["feat1", "feat2"], num_trees=0)

    def test_invalid_feature_subset_category(self):
        """Negative test for feature subset category"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"],
            feature_subset_category="any")

    def test_rand_forest_publish(self):
        """Test binomial classification of random forest model"""
        model = self.context.models.classification.random_forest_classifier.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        model.save("test/randomforestclassifier")
        restored = self.context.load("test/randomforestclassifier")
        print restored
        #self.assertIn("hdfs", path)
        #self.assertIn("tar", path)

if __name__ == '__main__':
    unittest.main()
