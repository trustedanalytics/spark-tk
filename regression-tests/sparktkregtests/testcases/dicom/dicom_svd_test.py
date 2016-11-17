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

