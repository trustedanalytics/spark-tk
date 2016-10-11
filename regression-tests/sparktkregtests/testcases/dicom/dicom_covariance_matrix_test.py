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

"""tests covariance of dicom images"""

import unittest
from sparktk import dtypes
from sparktkregtests.lib import sparktk_test
from numpy import cov
from numpy.linalg import svd
from numpy.testing import assert_almost_equal


class DicomCovarianceMatrixTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomCovarianceMatrixTest, self).setUp()
        dataset = self.get_file("dcm_images")
        dicom = self.context.dicom.import_dcm(dataset)
        self.frame = dicom.pixeldata
       
    def test_covariance_matrix(self):
        """Test the output of dicom_covariance_matrix"""
        self.frame.dicom_covariance_matrix("imagematrix")

        results = self.frame.to_pandas(self.frame.count())

        #compare result
        for i, row in results.iterrows():
            actual_cov = row['CovarianceMatrix_imagematrix']

            #expected ouput using numpy's covariance method
            expected_cov = cov(row['imagematrix'], rowvar=False)
            
            assert_almost_equal(actual_cov, expected_cov,
                                decimal=4, err_msg="cov incorrect")

    def test_invalid_column_name(self):
        """Test behavior for invalid column name"""
        with self.assertRaisesRegexp(
                Exception, "column ERR was not found"):
            self.frame.dicom_covariance_matrix("ERR")

    def test_invalid_param(self):
        """Test behavior for invalid parameter"""
        with self.assertRaisesRegexp(
                Exception, "takes exactly 2 arguments"):
            self.frame.svd("imagematrix", True)

if __name__ == "__main__":
    unittest.main()
