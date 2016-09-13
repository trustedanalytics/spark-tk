""" Computes covariance on 2048 vectors of 400 elements each """
import unittest
import numpy
import sparktk
from sparktkregtests.lib import sparktk_test


class CovarVect400ColX2KRowTest(sparktk_test.SparkTKTestCase):

    @unittest.skip("schema does not allow vector as data type")
    def test_covar_on_vectors(self):
        """Validate a matrix against manually computed values"""
        input_file = self.get_file("vector.csv")
        vect_schema = [("items", sparktk.dtypes.vector(400))]

        # create a frame and covariance matrix
        cov_frame = self.context.frame.import_csv(input_file,
                                                  schema=vect_schema)
        cov_matrix = cov_frame.covariance_matrix(['items'])

        # call numpy to get numpy result
        numpy_result = list(numpy.cov(list(cov_frame.take(cov_frame.count()).data),
                                      rowvar=False))

        # convert the frame rows into lists for ease of comparison
        sparktk_flat = list(numpy.array(cov_matrix.take(cov_matrix.count())).flat)
        numpy_flat = list(numpy.array(numpy_result).flat)

        # finally compare the expected results with those resturned by sparktk
        numpy.testing.assert_almost_equal(sparktk_flat, numpy_flat)

if __name__ == "__main__":
    unittest.main()
