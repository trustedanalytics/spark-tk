#############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
   Usage:  python2.7 export_to_hive_test.py
   Test functionality of:
    * frame.export_to_hive()
    * ia.HiveQuery()
"""

__author__ = "KH"
__credits__ = ["Karen Herrold"]
__version__ = "2015.08.10"

# There is no test case round-tripping data from Hive to ATK to Hive
# due to data type issues; however, Hive to ATK to Hive is tested.
#
# Functionality tested:
# * export_to_hive
# - default export
#
#   Error Tests:
#   - export to existing table
#   - export table containing vector
#
# * HiveQuery
# - default query (select *)
# - query on specific column
# - query on multiple specific columns
# - query on various data types (primitive, complex)
# - constrained query on all columns
# - query on joined tables
#
#   Error Tests:
#   - query on invalid table
#   - query on invalid column
#   - query on table containing char
#   - query on table containing binary
#   - query on table containing array (vector)
#

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import time
import datetime
import numpy as np

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import config
from qalib import atk_test


class HiveTest(atk_test.ATKTestCase):

    def setUp(self):
        """Initialize test data at beginning of test"""
        # Call parent set-up.
        super(HiveTest, self).setUp()
        white_schema = [("col_A", ia.int32),
                        ("col_B", ia.int64),
                        ("col_3", ia.float32),
                        ("Double", ia.float64),
                        ("Text", str)]

        dataset_delimT = "delimTest1.tsv"
        self.frame = frame_utils.build_frame(dataset_delimT, white_schema, delimit='\t')
        

    def test_export_to_hive_and_query(self):
        """
        test_export_to_hive_and_query: write frame to hive and read back.
        """
        # Add table name to list for clean up.  Do it first so if export
        # fails after creating table, table will still be cleaned up.
        tbl_name = common_utils.get_a_name("atk_table")
        self.frame.export_to_hive(tbl_name)

        # Now query it and verify data was unchanged.
        query = "select * from " + tbl_name
        hive_query = ia.HiveQuery(query)
        frame = ia.Frame(hive_query)
        # print "frame contains {0} rows.  frame:\n{1}" \
        #     .format(frame.row_count, frame.inspect(frame.row_count))
        self.assertTrue(
            frame_utils.compare_frames(frame, self.frame, zip(frame.column_names, self.frame.column_names)))

    def test_export_to_hive_extreme_and_query(self):
        """
        test_export_to_hive_extreme_and_query: write frame to hive & read back.
        """
        def add_col(row):
            if row["col_A"] > 5000:
                return [sys.maxint, sys.float_info.max]
            if row["col_A"] > 1000:
                return [-sys.maxint-1, sys.float_info.min]
            return [np.nan, np.nan]

        # Add columns for max, min & NaN for both int & float.
        self.frame.add_columns(add_col,
                               [('num', ia.float64),
                                ('numf', ia.float64)],
                               columns_accessed=['col_A'])

        # fails after creating table, table will still be cleaned up.
        tbl_name = common_utils.get_a_name("atk_table")
        self.frame.export_to_hive(tbl_name)

        # Now query it and verify data was unchanged.
        query = "select * from " + tbl_name
        hive_query = ia.HiveQuery(query)
        frame = ia.Frame(hive_query)
        self.assertTrue(
            frame_utils.compare_frames(frame, self.frame, zip(frame.column_names, self.frame.column_names), almost_equal=True))

    def test_export_to_hive_vector_error(self):
        """
        test_export_to_hive_query_vector_error: try to write vector to Hive.
        """
        tbl_name = common_utils.get_a_name("water_data")
        self.frame.add_columns(lambda row: [1.0, 2.0],
                               ("vector", ia.vector(2)))
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_hive,
                          tbl_name)

    def test_export_to_hive_table_exists_error(self):
        """
        test_export_to_hive_table_exists_error: write to existing table name.
        """
        tbl_name = common_utils.get_a_name("water_data")
	self.frame.export_to_hive(tbl_name)
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_hive,
                          tbl_name)

    def test_export_to_hive_table_null_error(self):
        """
        test_export_to_hive_table_null_error: write to null table name.
        """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_hive,
                          None)

    def test_hive_query_default(self):
        """
        test_hive_query_default: basic query on all fields.
        """
        # Select the individual fields since types binary and char are
        # not native in ATK and we need to exclude those for now.  Also
        # need to filter to get rid of header row.
        # query = "select * from " + self.water_data_name
        tbl_name = common_utils.get_a_name("water_data")
	self.frame.export_to_hive(tbl_name)
        query = "select col_A, col_B, col_3, Double, Text from " + tbl_name
        hive_query = ia.HiveQuery(query)
        water_data = ia.Frame(hive_query)

        self.assertTrue(frame_utils.compare_frames(water_data, self.frame, zip(water_data.column_names,  self.frame.column_names)))

    def test_hive_query_invalid_table_error(self):
        """
        test_hive_query_invalid_table_error: query on invalid table.
        """
        query = "select * from invalid_table"
        hive_query = ia.HiveQuery(query)
        self.assertRaises(ia.rest.command.CommandServerError,
                          ia.Frame,
                          hive_query)

    def test_hive_query_invalid_column_error(self):
        """
        test_hive_query_invalid_column_error: query on invalid column.
        """
        tbl_name = common_utils.get_a_name("water_data")
	self.frame.export_to_hive(tbl_name)
        query = "select invalid_column from " + tbl_name
        hive_query = ia.HiveQuery(query)
        self.assertRaises(ia.rest.command.CommandServerError,
                          ia.Frame,
                          hive_query)


if __name__ == "__main__":
    unittest.main()
