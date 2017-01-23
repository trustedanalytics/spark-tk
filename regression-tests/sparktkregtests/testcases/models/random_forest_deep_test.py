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


class RandomForestDeep(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForestDeep, self).setUp()

        schema = [("feat"+str(i), int) for i in range(1000)] + [("class", int)]
        filename = self.get_file("rand_forest_regress.csv")

        self.frame = self.context.frame.import_csv(filename, schema=schema)

    def test_deep_regression_tree(self):
        """ Test the regression model, forcing the model deeper than 32"""
        schema = ["feat"+str(i) for i in range(1000)]
        model = self.context.models.regression.random_forest_regressor.train( 
            self.frame, schema, "class", max_depth=34, seed=0)

        result = model.test(self.frame)
        # This result is determined experimentally, it was better than when
        # this test is run with a depth of 32. We use a fixed value in the
        # interest of expedience, this test is already quite long
        self.assertAlmostEqual(result.root_mean_squared_error, 0.0109544511501)

if __name__ == '__main__':
    unittest.main()
