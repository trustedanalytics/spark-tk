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

    @unittest.skip("Correlation matrix produces value different than numpy correl")
    def test_correl_matrix(self):
        """Verify correlation matrix on all columns"""
        correl_matrix = self.base_frame.correlation_matrix(self.base_frame.column_names)
        numpy_correl = list(numpy.ma.corrcoef(list(self.base_frame.take(self.base_frame.count())),
                                              rowvar=False))

        # convert to lists for ease of comparison
        correl_flat = list(numpy.array(correl_matrix.take(correl_matrix.count())).flat)
        numpy_correl = list(numpy.array(numpy_correl).flat)

        # compare the correl matrix values with the expected results
        for correl_value, ref_value in zip(correl_flat, numpy_correl):
            self.assertAlmostEqual(correl_value, ref_value, 5)


if __name__ == "__main__":
    unittest.main()
