# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""Tests svd on dicom frame"""

import unittest
from sparktk import dtypes
from sparktkregtests.lib import sparktk_test
from numpy.linalg import svd
from numpy.testing import assert_almost_equal


class SVDDicomTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(SVDDicomTest, self).setUp()
        dataset = self.get_file("dicom_uncompressed")
        dicom = self.context.dicom.import_dcm(dataset)
        self.frame = dicom.pixeldata

    def test_svd(self):
        """Test the output of svd"""
        self.frame.matrix_svd("imagematrix")

        #get pandas frame of the output
        results = self.frame.to_pandas(self.frame.count())

        #compare U,V and s matrices for each image against numpy's output
        for i, row in results.iterrows():
            actual_U = row['U_imagematrix']
            actual_V = row['V_imagematrix']
            actual_s = row['SingularVectors_imagematrix']

            #expected ouput using numpy's svd
            U, s, V = svd(row['imagematrix'])

            assert_almost_equal(actual_U, U, decimal=4, err_msg="U incorrect")
            assert_almost_equal(actual_V, V.T, decimal=4, err_msg="V incorrect")
            assert_almost_equal(
                    actual_s[0], s, decimal=4, err_msg="Singual vectors incorrect")

    def test_invalid_column_name(self):
        """Test behavior for invalid column name"""
        with self.assertRaisesRegexp(
                Exception, "column ERR was not found"):
            self.frame.matrix_svd("ERR")

    def test_invalid_param(self):
        """Test behavior for invalid parameter"""
        with self.assertRaisesRegexp(
                Exception, "svd\(\) takes exactly 2 arguments"):
            self.frame.matrix_svd("imagematrix", True)
if __name__ == "__main__":
    unittest.main()