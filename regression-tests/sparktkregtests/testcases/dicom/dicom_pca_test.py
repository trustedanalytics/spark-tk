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

"""tests pca on dicom frame"""

import unittest
from sparktk import dtypes
from sparktkregtests.lib import sparktk_test
from numpy.linalg import svd
from numpy.testing import assert_almost_equal


class DicomPCATest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """import dicom data for testing"""
        super(DicomPCATest, self).setUp()
        dataset = self.get_file("dicom_uncompressed")
        dicom = self.context.dicom.import_dcm(dataset)
        frame = dicom.pixeldata
        #rename to self.frame after bug fix
        self.frame = dicom.pixeldata

        #temporary fix until the bug is fixed
        #Will be removed after the bug fix
        #pixeldata_df = frame.to_pandas(frame.count())
        #self.frame = self.context.frame.create(
        #    [[pixeldata_df['imagematrix'][0]],
        #    [pixeldata_df['imagematrix'][1]],
        #    [pixeldata_df['imagematrix'][2]]],
        #    schema=[("imagematrix", dtypes.matrix)])

        #perform svd on the frame to get V matrix
        self.frame.matrix_svd("imagematrix")

    def test_PCA(self):
        """Test the output of pca"""
        self.frame.matrix_pca("imagematrix", "V_imagematrix")

        results = self.frame.to_pandas(self.frame.count())

        #compare against expected output
        for i, row in results.iterrows():
            actual_pcs = row['PrincipalComponents_imagematrix']

            #V matrix from numpy's svd
            U, s, V = svd(row['imagematrix'])
            expected_pcs = row['imagematrix'] * V.T

            assert_almost_equal(actual_pcs, expected_pcs,
                                decimal=4, err_msg="pcs incorrect")

    def test_invalid_column_name(self):
        """Test behavior for invalid column name"""
        with self.assertRaisesRegexp(
                Exception, "column ERR was not found"):
            self.frame.matrix_pca("ERR", "V_imagematrix")

    def test_missing_V(self):
        """Test behavior for missing V matrix column name"""
        with self.assertRaisesRegexp(
                Exception, "takes exactly 3 arguments"):
            self.frame.matrix_pca("imagematrix")

    def test_invalid_V_name(self):
        """Test behavior for invalid column name"""
        with self.assertRaisesRegexp(
                Exception, "column V was not found"):
            self.frame.matrix_pca("imagematrix", "V")

if __name__ == "__main__":
    unittest.main()
