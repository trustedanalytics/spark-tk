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
        pandaframe = self.frame.download()
        pandaframe.logM = pandaframe.logM.astype(np.float64)
        pandaframe.Rs = pandaframe.Rs.astype(np.float64)
        pandaframe.Rl = pandaframe.Rl.astype(np.float64)
        #Durbin Watson
        db_result = smst.durbin_watson(pandaframe["logM"])

        self.assertAlmostEqual(result, db_result, delta=0.0000000001)

if __name__ == "__main__":
    unittest.main()
