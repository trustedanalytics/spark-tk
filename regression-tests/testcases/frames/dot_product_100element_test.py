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

""" Dot product of 100 by 100 vectors, compared to seperate implementation"""
import unittest
import sparktk
import sys
import os
import numpy
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

# Related bugs:
# @DPNG-9854 schema does not allow vector datatype

class DotProduct100ElementTest(sparktk_test.SparkTKTestCase):

    def test_dot_product_100elements(self):
        dataset = self.get_file("dot_prod_100D_vect_36Rows.csv")
        schema = [("Vect_A", sparktk.dtypes.vector(100)),
                  ("Vect_B", sparktk.dtypes.vector(100)),
                  ("Base", float)]
        # currently this will fail as vector is not a supported datatype for schema, bug filed
        frame = self.context.frame.import_csv(dataset, schema=schema, header=True)
        frame.dot_product(["Vect_A"], ["Vect_B"], "Dot_prod")

        results = frame.download(frame.row_count)

        for _, i in results.iterrows():
            self.assertAlmostEqual(i["Base"], i["Dot_prod"], delta=1e11)

if __name__ == "__main__":
    unittest.main()
