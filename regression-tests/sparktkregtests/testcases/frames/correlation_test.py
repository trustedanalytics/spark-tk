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


class CorrelationTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(CorrelationTest, self).setUp()
        data_in = self.get_file("covariance_correlation.csv")
        self.base_frame = self.context.frame.import_csv(data_in)

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
        correl_matrix = self.base_frame.correlation_matrix(self.base_frame.column_names)
        numpy_correl = numpy.ma.corrcoef(list(self.base_frame.take(self.base_frame.count())),
                                              rowvar=False)
        print "our result: " + str(correl_matrix.to_pandas(5))
        print "numpy result: " + str(numpy_correl)

        # compare the correl matrix values with the expected results
        for i, j in zip(len(correl_matrix), len(correl_matrix[0])):
            if i == j:
                self.assertEqual(correl_matrix[i][j], 1)
            else:
                self.assertAlmostEqual(correl_matrix[i][j], numpy_correl[i][j])


if __name__ == "__main__":
    unittest.main()
