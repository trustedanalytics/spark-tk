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

""" performance test cases for multivariate cox proportional hazard"""

import unittest

from sparktkregtests.lib import sparktk_test
from sparktkregtests.lib import performance_utils as profiler


class CoxPerformance(sparktk_test.SparkTKTestCase):

    def test_cox_ph(self):
        """Train and predict on an 8 covariate time series"""

        schema = [("censor", int),
                  ("value", float),
                  ("Vec1", float),
                  ("Vec2", float),
                  ("Vec3", float),
                  ("Vec4", float),
                  ("Vec5", float),
                  ("Vec6", float),
                  ("Vec7", float),
                  ("Vec8", float)]

        ds = self.get_file(self.id(), True)
        self.frame_train = self.context.frame.import_csv(ds, schema=schema)

        with profiler.Timer("profile." + self.id() + "_train"):
            cox_ph = self.context.models.survivalanalysis.cox_ph.train(
                self.frame_train, "value",
                ["Vec1", "Vec2", "Vec3", "Vec4",
                 "Vec5", "Vec6", "Vec7", "Vec8"], "censor")

        with profiler.Timer("profile." + self.id() + "_predict"):
            cox_ph.predict(self.frame_train)
    
    unittest.skip("fails due convergence error")
    def test_cox_ph_wide(self):
        """Train and predict on an 100 covariate time series"""
        train_vecs = ["Vec"+str(i) for i in xrange(100)]

        schema = [("censor", int),
                  ("value", float)] + map(lambda x: (x, float), train_vecs)

        ds = self.get_file(self.id(), True)
        self.frame_train = self.context.frame.import_csv(ds, schema=schema)

        with profiler.Timer("profile." + self.id() + "_train"):
            cox_ph = self.context.models.survivalanalysis.cox_ph.train(
                self.frame_train, "value", train_vecs, "censor")

        with profiler.Timer("profile." + self.id() + "_predict"):
            cox_ph.predict(self.frame_train)

    unittest.skip("fails due to out of memory")
    def test_cox_ph_ultra_wide(self):
        """Train and predict on an 100 covariate time series"""
        train_vecs = ["Vec"+str(i) for i in xrange(1000)]

        schema = [("censor", int),
                  ("value", float)] + map(lambda x: (x, float), train_vecs)

        ds = self.get_file(self.id(), True)
        self.frame_train = self.context.frame.import_csv(ds, schema=schema)

        with profiler.Timer("profile." + self.id() + "_train"):
            cox_ph = self.context.models.survivalanalysis.cox_ph.train(
                self.frame_train, "value", train_vecs, "censor")

        with profiler.Timer("profile." + self.id() + "_predict"):
            cox_ph.predict(self.frame_train)


if __name__ == '__main__':
    unittest.main()
