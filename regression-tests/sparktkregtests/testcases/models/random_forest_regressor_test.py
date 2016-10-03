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
