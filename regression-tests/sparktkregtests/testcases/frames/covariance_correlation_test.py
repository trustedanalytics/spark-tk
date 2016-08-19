"""Test covariance and correlation on 2 columns, matrices on 400x1024 matric"""
import unittest
import numpy
from sparktkregtests.lib import sparktk_test


class CovarianceCorrelationTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(CovarianceCorrelationTest, self).setUp()
        data_in = self.get_file("covariance_correlation.csv")
        self.base_frame = self.context.frame.import_csv(data_in)

    def test_covar(self):
        """Test covariance between 2 columns"""
        sparktk_result = self.base_frame.covariance('C1','C4')
        C1_C4_columns_data = self.base_frame.take(self.base_frame.row_count, columns=["C1", "C4"]).data
        numpy_result = numpy.cov(list(C1_C4_columns_data), rowvar=False)

        self.assertAlmostEqual(sparktk_result, float(numpy_result[0][1]))

    def test_correl(self):
        """Test correlation between 2 columns"""
        correl_1_3 = self.base_frame.correlation('C0', 'C2')
        C1_C3_columns_data = self.base_frame.take(self.base_frame.row_count, columns=["C0", "C2"]).data
        numpy_result = numpy.ma.corrcoef(list(C1_C3_columns_data), rowvar=False)

        self.assertAlmostEqual(correl_1_3, float(numpy_result[0][1]))

    def test_covar_matrix(self):
        """Verify covariance matrix on all columns"""
        # create covariance matrix using sparktk
        covar_matrix = self.base_frame.covariance_matrix(self.base_frame.column_names)

        # convert to list for ease of comparison
        covar_flat = list(numpy.array(covar_matrix.take(covar_matrix.row_count).data).flat)
        numpy_covar_result = list(numpy.cov(list(self.base_frame.take(self.base_frame.row_count).data), rowvar=False))
        # flatten the numpy result
        numpy_covar_result = list(numpy.array(numpy_covar_result).flat)
        
        # ensure that the data in the covar matrix
        # matches that which numpy gave us (expected results)
        for (spark_tk_row, numpy_row) in zip(covar_flat, numpy_covar_result):
            self.assertAlmostEqual(spark_tk_row, numpy_row)
    
    def test_correl_matrix(self):
        """Verify correlation matrix on all columns"""
        correl_matrix = self.base_frame.correlation_matrix(self.base_frame.column_names)
        numpy_correl = list(numpy.ma.corrcoef(list(self.base_frame.take(self.base_frame.row_count).data), rowvar=False))
        # convert to lists for ease of comparison
        correl_flat = list(numpy.array(correl_matrix.take(correl_matrix.row_count).data).flat)
        numpy_correl = list(numpy.array(numpy_correl).flat)
        print "correl matrix sparktk: " + str(correl_flat)
        print "numpy result: " + str(numpy_correl)
        # compare the correl matrix values with the expected results
        for correl_value, ref_value in zip(correl_flat, numpy_correl):
            self.assertAlmostEqual(correl_value, ref_value, 5)


if __name__ == "__main__":
    unittest.main()
