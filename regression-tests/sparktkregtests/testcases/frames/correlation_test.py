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

"""Test covariance and correlation on 2 columns, matrices on 400x1024 matric"""
import unittest
import numpy
from sparktkregtests.lib import sparktk_test
import math


class CorrelationTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(CorrelationTest, self).setUp()
        data_in = self.get_file("covariance_correlation.csv")
        self.base_frame = self.context.frame.import_csv(data_in)
        self.count = self.base_frame.count()

    def test_correl(self):
        """Test correlation between 2 columns"""
        correl_0_2 = self.base_frame.correlation('C0', 'C2')
        C0_C2_columns_data = self.base_frame.take(self.base_frame.count(),
                                                  columns=["C0", "C2"])
        numpy_result = numpy.ma.corrcoef(list(C0_C2_columns_data),
                                         rowvar=False)

        self.assertAlmostEqual(correl_0_2, float(numpy_result[0][1]))

    def test_correl_matrix(self):
        """Verify correlation matrix on all columns"""
        correl_matrix = self.base_frame.correlation_matrix(self.base_frame.column_names).take(self.count)
        numpy_correl = numpy.ma.corrcoef(list(self.base_frame.take(self.base_frame.count())),
                                              rowvar=False)

        # compare the correl matrix values with the expected results
        for i in range(0, len(correl_matrix)):
            for j in range(0, len(correl_matrix[0])):
                if i == j:
                    self.assertEqual(correl_matrix[i][j], 1)
                elif numpy_correl[i][j] is numpy.ma.masked:
                    self.assertTrue(math.isnan(correl_matrix[i][j]))
                else:
                    self.assertAlmostEqual(correl_matrix[i][j], numpy_correl[i][j])


if __name__ == "__main__":
    unittest.main()
