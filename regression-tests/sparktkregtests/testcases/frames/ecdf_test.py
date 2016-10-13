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

""" Tests the ECDF functionality """
import unittest
import random
from sparktkregtests.lib import sparktk_test


class ecdfTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(ecdfTest, self).setUp()

        # generate a dataset to test ecdf on
        # it will just be a single column of ints
        column = [[random.randint(0, 5)] for index in xrange(0, 20)]
        schema = [("C0", int)]

        self.frame = self.context.frame.create(column,
                                               schema=schema)

    def validate_ecdf(self):
        # call sparktk ecdf function on the data and get as pandas df
        ecdf_sparktk_result = self.frame.ecdf("C0")
        pd_ecdf = ecdf_sparktk_result.to_pandas(ecdf_sparktk_result.row_count)
        # get the original frame as pandas df so we can calculate our own result
        pd_original_frame = self.frame.to_pandas(self.frame.row_count)

        # the formula for calculating ecdf is
        # F(x) = 1/n * sum from 1 to n of I(x_i)
        # where I = { 1 if x_i <= x, 0 if x_i > x }
        # i.e., for each element in our data column count
        # the number of items in that row which are less than
        # or equal to that item, divide by the number
        # of total items in the column
        grouped = pd_original_frame.groupby("C0").size()
        our_result = grouped.sort_index().cumsum()*1.0/len(pd_original_frame)

        # finaly we iterate through the sparktk result and compare it with our result
        for index, row in pd_ecdf.iterrows():
            self.assertAlmostEqual(row["C0"+'_ecdf'],
                                   our_result[int(row["C0"])])

    def test_ecdf_bad_name(self):
        """Test ecdf with an invalid column name."""
        with self.assertRaisesRegexp(Exception, "No column named bad_name"):
            self.frame.ecdf("bad_name")

    def test_ecdf_bad_type(self):
        """Test ecdf with an invalid column type."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.frame.ecdf(5)

    def test_ecdf_none(self):
        """Test ecdf with a None for the column name."""
        with self.assertRaisesRegexp(Exception, "column is required"):
            self.frame.ecdf(None)


if __name__ == '__main__':
    unittest.main()
