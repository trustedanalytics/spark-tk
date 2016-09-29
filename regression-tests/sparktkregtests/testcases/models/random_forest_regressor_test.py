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

    def test_train(self):
        """Test random forest train method"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        
        self.assertEqual(model.max_bins, 100)
        self.assertEqual(model.max_depth, 4)
        self.assertEqual(model.num_classes, 2)
        self.assertEqual(model.num_trees, 1)
        self.assertEqual(model.impurity, 'variance')

    def test_predict(self):
        """Test predicted values are correct"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)

        model.predict(self.frame)
        preddf = self.frame.to_pandas(self.frame.count())
        for index, row in preddf.iterrows():
            self.assertAlmostEqual(float(row['class']), row['predicted_value'])

    @unittest.skip("not implemented")
    def test_publish(self):
        """Test publish plugin"""
        model = self.context.models.regression.random_forest_regressor.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        path = model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

if __name__ == '__main__':
    unittest.main()
