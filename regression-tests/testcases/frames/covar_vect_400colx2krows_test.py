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
import sys
import os
import numpy
import sparktk
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

# Related bugs:
# @DPNG-9854 schema does not support vector type

class CovarVect400ColX2KRowTest(sparktk_test.SparkTKTestCase):

    def test_covar_on_vectors(self):
        """Validate a matrix against manually computed values"""

        input_file = self.get_file("Covar_vector_400Elem_2KRows.csv")
        reference_file = self.get_file("Covar_vect_400sq_baseline_v01.txt")
        vect_schema = [("items", sparktk.dtypes.vector(400))]

        # this will fail because schema does not currently accept the vector type
        cov_frame = self.context.frame.import_csv(input_file, schema=vect_schema)
        cov_matrix = cov_frame.covariance_matrix(['items'])

        # Create schema for 400-column reference file/frame.
        sch2 = [("col_" + str(i), float) for i in range(1, 401)]

        # Build a frame from the pre-computed values for this dataset.
        ref_frame = self.context.frame.import_csv(reference_file, schema=sch2)

        baseline_flat = list(numpy.array(ref_frame.take(ref_frame.row_count)).flat)
        computed_flat = list(numpy.array(cov_matrix.take(cov_matrix.row_count)).flat)

        for base, computed in zip(baseline_flat, computed_flat):
            self.assertAlmostEqual(computed, base, 7)

if __name__ == "__main__":
    unittest.main()
