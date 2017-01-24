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
        numpy_result = list(numpy.cov(list(
            cov_frame.take(cov_frame.count()).data), rowvar=False))

        # convert the frame rows into lists for ease of comparison
        sparktk_flat = list(numpy.array(
            cov_matrix.take(cov_matrix.count())).flat)
        numpy_flat = list(numpy.array(numpy_result).flat)

        # finally compare the expected results with those resturned by sparktk
        numpy.testing.assert_almost_equal(sparktk_flat, numpy_flat)


if __name__ == "__main__":
    unittest.main()
