"""Test covariance and correlation on 2 columns, matrices on 400x1024 matric"""
import unittest
import numpy
from sparktkregtests.lib import sparktk_test


class CovarianceCorrelationTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(CovarianceCorrelationTest, self).setUp()

        data_in = self.get_file("Covariance_400Col_1K.02_without_spaces.csv")
        self.base_frame = self.context.frame.import_csv(data_in)

    def test_covar(self):
        """Test covariance between 2 columns"""
        print "schema: " + str(self.base_frame.schema)
        covar_2_5 = self.base_frame.covariance('C2','C5')
        self.assertEqual(covar_2_5, -5.25513196480937949673)

    def test_correl(self):
        """Test correlation between 2 columns"""
        correl_1_3 = self.base_frame.correlation('C1', 'C3')
        self.assertEqual(correl_1_3, 0.124996244847393425669857)

    def test_covar_matrix(self):
        """Verify covariance matrix on all columns"""
        covar_result = self.get_file("Covariance_400sq_ref.02.txt")
        covar_ref_frame = self.context.frame.import_csv(covar_result)
        covar_matrix = self.base_frame.covariance_matrix(self.base_frame.column_names)
        covar_flat = list(numpy.array(covar_matrix.take(covar_matrix.row_count).data).flat)
        covar_ref_flat = list(numpy.array(covar_ref_frame.take(covar_ref_frame.row_count).data).flat)
        for cov_value, res_value in zip(covar_flat, covar_ref_flat):
            self.assertAlmostEqual(cov_value, res_value, 7)

    def test_correl_matrix(self):
        """Verify correlation matrix on all columns"""
        correl_matrix = self.base_frame.correlation_matrix(self.base_frame.column_names)
        correl_result = self.get_file("Correlation_400sq_ref.02.txt")
        correl_ref_frame = self.context.frame.import_csv(correl_result)
        correl_flat = list(numpy.array(correl_matrix.take(correl_matrix.row_count).data).flat)
        cor_ref_flat = list(numpy.array(correl_ref_frame.take(correl_ref_frame.row_count).data).flat)
        for correl_value, ref_value in zip(correl_flat, cor_ref_flat):
            self.assertAlmostEqual(correl_value, ref_value, 5)


if __name__ == "__main__":
    unittest.main()
