""" Computes covariance on 2048 vectors of 400 elements each """
import unittest
import numpy
import sparktk
from sparktkregtests.lib import sparktk_test


class CovarVect400ColX2KRowTest(sparktk_test.SparkTKTestCase):

    @unittest.skip("schema does not allow vector as data type")
    def test_covar_on_vectors(self):
        """Validate a matrix against manually computed values"""
        input_file = self.get_file("Covar_vector_400Elem_2KRows.csv")
        # my understanding is that the reference file
        # contains the expected values that the original
        # author was looking for
        reference_file = self.get_file("Covar_vect_400sq_baseline_v01.txt")
        vect_schema = [("items", sparktk.dtypes.vector(400))]

        # create a frame and covariance matrix
        cov_frame = self.context.frame.import_csv(input_file,
                schema=vect_schema)
        cov_matrix = cov_frame.covariance_matrix(['items'])

        # Create schema for 400-column reference file/frame
        sch2 = [("col_" + str(i), float) for i in range(1, 401)]

        # Build a frame from the pre-computed values for this dataset
        ref_frame = self.context.frame.import_csv(reference_file, schema=sch2)

        # convert the frame rows into lists for ease of comparison
        baseline_flat = list(numpy.array(ref_frame.take(ref_frame.row_count)).flat)
        computed_flat = list(numpy.array(cov_matrix.take(cov_matrix.row_count)).flat)

        # finally compare the expected results with those resturned by sparktk
        for base, computed in zip(baseline_flat, computed_flat):
            self.assertAlmostEqual(computed, base, 7)


if __name__ == "__main__":
    unittest.main()
