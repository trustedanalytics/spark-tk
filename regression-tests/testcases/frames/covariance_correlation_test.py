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
import sys
import numpy
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

# related bugs:
# @DPNG-9815 flatten should allow columns as a list

class CovarianceCorrelationTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(CovarianceCorrelationTest, self).setUp()

        data_in = self.get_file("Covariance_400Col_1K.02_without_spaces.csv")
        schema_in = []
        for i in range(1, 301):
            col_spec = ('col_' + str(i), int)
            schema_in.append(col_spec)
        for i in range(301, 401):
            col_spec = ('col_' + str(i), float)
            schema_in.append(col_spec)

        self.schema_result = [('col_'+str(i), float)
                              for i in range(1, 401)]

        # Define a list of column names to be used in creating the covariance
        # and correlation matrices.
        self.columns = ["col_"+str(i) for i in range(1, 401)]
        self.base_frame = self.context.frame.import_csv(data_in, schema=schema_in)

    def test_covar(self):
        """Test covariance between 2 columns"""
        covar_2_5 = self.base_frame.covariance('col_2','col_5')
        self.assertEqual(covar_2_5, -5.25513196480937949673)

    def test_correl(self):
        """Test correlation between 2 columns"""
        correl_1_3 = self.base_frame.correlation('col_1', 'col_3')
        self.assertEqual(correl_1_3, 0.124996244847393425669857)

    def test_covar_matrix(self):
        """Verify covariance matrix on all columns"""
        covar_result = self.get_file("Covariance_400sq_ref.02.txt")
        covar_ref_frame = self.context.frame.import_csv(covar_result, schema=self.schema_result)
        covar_matrix = self.base_frame.covariance_matrix(self.columns)
        covar_flat = list(numpy.array(covar_matrix.take(covar_matrix.row_count).data).flat)
        covar_ref_flat = list(numpy.array(covar_ref_frame.take(covar_ref_frame.row_count).data).flat)
        for cov_value, res_value in zip(covar_flat, covar_ref_flat):
            self.assertAlmostEqual(cov_value, res_value, 7)

    def test_correl_matrix(self):
        """Verify correlation matrix on all columns"""
        correl_matrix = self.base_frame.correlation_matrix(self.columns)
        correl_result = self.get_file("Correlation_400sq_ref.02.txt")
        correl_ref_frame = self.context.frame.import_csv(correl_result, schema=self.schema_result)
        correl_flat = list(numpy.array(correl_matrix.take(correl_matrix.row_count).data).flat)
        cor_ref_flat = list(numpy.array(correl_ref_frame.take(correl_ref_frame.row_count).data).flat)
        for correl_value, ref_value in zip(correl_flat, cor_ref_flat):
            self.assertAlmostEqual(correl_value, ref_value, 5)


if __name__ == "__main__":
    unittest.main()
