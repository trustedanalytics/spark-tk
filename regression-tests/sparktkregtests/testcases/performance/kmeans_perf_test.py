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

""" performance test cases for the kmeans clustering algorithm """

import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import performance_utils as profiler

class PerformanceKMeans(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to test against."""
        super(PerformanceKMeans, self).setUp()

        schema = [("Vec1", float),
                  ("Vec2", float),
                  ("Vec3", float),
                  ("Vec4", float),
                  ("Vec5", float),
                  ("term", str)]

        ds = self.get_file(self.id(), performance_file=True)
        self.frame_train = self.context.frame.import_csv(ds, schema=schema)

    def test_kmeans_5by5(self):
        """Train a 5-feature, 5-class KMeans model"""
        with profiler.Timer("profile." + self.id() + "_train") as tmr:
            kmodel = self.context.models.clustering.kmeans.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], 5)

        with profiler.Timer("profile." + self.id() + "_predict") as tmr:
            kmodel.predict(self.frame_train)


if __name__ == '__main__':
    unittest.main()
