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

"""Tests frame timeseries tests """

import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test

import random

import pandas
import numpy as np
import statsmodels.stats.stattools as smst

#Change filename to the location of the csv file used for the test
filename = "../datasets/timeseriesstats.csv"


#from other file

class FrameInspectTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameInspectTest, self).setUp()

        dataset = self.get_file("timeseriesstats.csv")
        schema = [("year", float),
                  ("logM", float),
                  ("logYp", float),
                  ("Rs", float),
                  ("Rl", float),
                  ("Rm", float),
                  ("logSpp", float)]

        self.frame = self.context.frame.import_csv(
            dataset, delimiter= ' ', header=True, schema=schema)

    def test_frame_timeseries_durbin_watson(self):
        """Test Durbin Watson"""
        result = self.frame.timeseries_durbin_watson_test("logM")
        #pandaframe = pandas.DataFrame(new_data, columns=["year", "logM", "logYp", "Rs", "Rl", "Rm", "logSpp"])
        pandaframe = self.frame.to_pandas()
        pandaframe.logM = pandaframe.logM.astype(np.float64)
        pandaframe.Rs = pandaframe.Rs.astype(np.float64)
        pandaframe.Rl = pandaframe.Rl.astype(np.float64)
        #Durbin Watson
        db_result = smst.durbin_watson(pandaframe["logM"])

        self.assertAlmostEqual(result, db_result, delta=0.0000000001)

if __name__ == "__main__":
    unittest.main()
