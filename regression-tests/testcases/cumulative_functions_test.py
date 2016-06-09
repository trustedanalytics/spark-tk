##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
    Usage:  python2.7 cumulative_functions_test.py
    Test column accumulation methods.
"""
__author__ = 'Prune Wickart'
__credits__ = ["Venky Bharadwaj", "Prune Wickart"]
__version__ = "2015.01.20"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia
from qalib import frame_utils
from qalib import atk_test


class TestCumulativeFunctions(atk_test.ATKTestCase):

    def setUp(self):
        super(TestCumulativeFunctions, self).setUp()

        self.data_sum = "cumulative_seq_v2.csv"
        self.data_tally = "cumu_tally_seq.csv"
        self.schema_sum = [("sequence", ia.int32),
                           ("col1", ia.int32),
                           ("cumulative_sum", ia.int32),
                           ("percent_sum", ia.float64)]
        self.schema_tally = [("sequence", ia.int32),
                             ("user_id", ia.int32),
                             ("vertex_type", str),
                             ("movie_id", ia.int32),
                             ("rating", ia.int32),
                             ("splits", str),
                             ("count", ia.int32),
                             ("percent_count", ia.float64)]

    # @unittest.skip("https://jira01.devtools.intel.com/browse/TRIB-4377")
    def test_cumulative_sum_and_percent(self):
        frame = frame_utils.build_frame(self.data_sum, self.schema_sum)

        # Ensure that the frame is in the original order before each operation.
        frame.sort('sequence')
        frame.cumulative_sum("col1")
        frame.sort('sequence')
        frame.cumulative_percent("col1")
        frame.sort('sequence')
        print frame.inspect(10)
        print frame.inspect(10, frame.row_count-9)
        # Write the frame to a log file for developer use.
        # log = open("TRIB-4377-rehash.log", "w")
        # for row in frame.take(frame.row_count):
        #     log.write(str(row) + '\n')
        #     print row
        # log.close()

        frame_row_count = frame.row_count
        print "The number of rows in original frame before cumulative sum  = %s" \
              % frame_row_count

        # Filter out any rows where the counts don't match
        #   the stored expected results.
        frame.filter(lambda row: (row.cumulative_sum ==
                                  row.col1_cumulative_sum) and
                     abs(row.percent_sum - row.col1_cumulative_percent)
                     < 0.000001)
        frame_sum_row_count = frame.row_count
        print "Rows in the cumulative frame after filter = %s" % \
              frame_sum_row_count
        # print frame.inspect(7, 21025)

        self.assertEqual(frame_row_count, frame_sum_row_count,
                         "https://intel-data.atlassian.net/browse/DPAT-245")

    def test_cumulative_colname_collision(self):
        frame = frame_utils.build_frame(self.data_sum, self.schema_sum)

        # Column name already exists
        # Add a column of zeroes; try the sum on remaining data
        # Extend the test for the "back-up" column name, too.
        frame.add_columns(
            lambda row: (0, 0), [("col1_cumulative_sum", ia.float32),
                                 ("col1_cumulative_sum_0", ia.float32)])
        frame.add_columns(
            lambda row: (0, 0), [("col1_cumulative_percent", ia.float32),
                                 ("col1_cumulative_percent_0", ia.float32)])
        frame_width = len(frame.column_names)
        frame.cumulative_sum("col1")
        frame.cumulative_percent("col1")
        print frame.inspect()
        self.assertEqual(frame_width+2, len(frame.column_names))

    def test_cumulative_bad_colname(self):
        frame = frame_utils.build_frame(self.data_sum, self.schema_sum)

        # Non-existent column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.cumulative_sum, "no_such_column")

        # None column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.cumulative_sum, None)

        # Non-existent column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.cumulative_percent, "no_such_column")

        # None column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.cumulative_percent, None)

    def test_tally_and_tally_percent(self):
        frame = frame_utils.build_frame(self.data_tally, self.schema_tally)

        # Ensure that the frame is in the original order before each operation.
        frame.sort('sequence')
        frame.tally("rating", '5')
        frame.sort('sequence')
        frame.tally_percent("rating", '5')
        frame.sort('sequence')
        print frame.inspect(10, 1000)

        frame_row_count = frame.row_count
        print "The number of rows in original frame before tally  = %s" \
              % frame_row_count

        # Filter out any rows where the counts don't match
        #   the stored expected results.
        frame.filter(lambda row: row.count == row.rating_tally and
                     abs(row.percent_count - row.rating_tally_percent)
                     < 0.000001)
        frame_tally_rows = frame.row_count
        print "Rows in the tally frame after filter %s" % frame_tally_rows
        print frame.inspect(10, 1000)

        self.assertEqual(frame_row_count, frame_tally_rows,
                         "https://intel-data.atlassian.net/browse/DPAT-245")

    def test_tally_colname_collision(self):
        frame = frame_utils.build_frame(self.data_tally, self.schema_tally)

        # Column name already exists
        # Add a column of zeroes; try the sum on remaining data
        # Extend the test for the "back-up" column name, too.
        frame.add_columns(
            lambda row: (0, 0), [("rating_tally", ia.float32),
                                 ("rating_tally_0", ia.float32)])
        frame.add_columns(
            lambda row: (0, 0), [("rating_tally_percent", ia.float32),
                                 ("rating_tally_percent_0", ia.float32)])
        frame_width = len(frame.column_names)
        frame.tally("rating", '5')
        frame.tally_percent("rating", '5')
        print frame.inspect()
        self.assertEqual(frame_width+2, len(frame.column_names))

    def test_tally_error(self):
        frame = frame_utils.build_frame(self.data_tally, self.schema_tally)

        # Non-existent column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally, "no_such_column", '5')

        # None column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally, None, '5')

        # Numeric target value
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally, "rating", 5)

        # None target value
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally, "rating", None)

        # Non-existent column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally_percent, "no_such_column", '5')

        # None column name
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally_percent, None, '5')

        # Numeric target value
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally_percent, "rating", 5)

        # None target value
        self.assertRaises(ia.rest.command.CommandServerError,
                          frame.tally_percent, "rating", None)

    def test_tally_no_element(self):
        """test when the element being tallied isn't present"""
        frame = frame_utils.build_frame(self.data_tally, self.schema_tally)
        frame.tally_percent("rating", "12")
        local_frame = frame.download(frame.row_count)

        for _, i in local_frame.iterrows():
            self.assertEqual(i["rating_tally_percent"], 1.0)

        
if __name__ == '__main__':
    unittest.main()
