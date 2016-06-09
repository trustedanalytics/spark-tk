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
   Usage:  python2.7 hbase_test.py
   Test functionality of:
    * frame.export_to_hbase()
    * ia.HbaseTable()
"""

__author__ = "KH"
__credits__ = ["Karen Herrold"]
__version__ = "2015.08.10"

# Exports data to HBase and then imports it back

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


class HBaseTest(atk_test.ATKTestCase):

    def setUp(self):
        """Initialize test data at beginning of test"""
        # Call parent set-up.
        super(HBaseTest, self).setUp()
        self.white_schema = [("col_A", ia.int32),
                             ("col_B", ia.int64),
                             ("col_3", ia.float32),
                             ("Double", ia.float64),
                             ("Text", str)]

        dataset_delimT = "delimTest1.tsv"
        self.frame = frame_utils.build_frame(dataset_delimT, self.white_schema, delimit='\t')
        

    def test_export_to_hbase_import(self):
        """ export and import a frame from hbase, see they are the same """
        tbl_name = common_utils.get_a_name("atk_table")
        schema = [("familyColumn", "col_A", ia.int32),
                  ("familyColumn", "col_B", ia.int64),
                  ("familyColumn", "col_3", ia.float32),
                  ("familyColumn", "Double", ia.float64),
                  ("familyColumn", "Text", str)]
        self.frame.export_to_hbase(tbl_name)

        hbase = ia.HBaseTable(tbl_name, schema)
        frame = ia.Frame(hbase)

        self.assertTrue(
            frame_utils.compare_frames(frame, self.frame, zip(frame.column_names, self.frame.column_names), almost_equal=True))

    def test_export_to_hbase_import_family(self):
        """ export and import a frame from hbase, see they are the same """
        tbl_name = common_utils.get_a_name("atk_table")
        schema = [("x", "col_A", ia.int32),
                  ("x", "col_B", ia.int64),
                  ("x", "col_3", ia.float32),
                  ("x", "Double", ia.float64),
                  ("x", "Text", str)]
        self.frame.export_to_hbase(tbl_name, family_name="x")

        hbase = ia.HBaseTable(tbl_name, schema)
        frame = ia.Frame(hbase)

        self.assertTrue(
            frame_utils.compare_frames(frame, self.frame, zip(frame.column_names, self.frame.column_names), almost_equal=True))

    def test_export_to_hbase_extreme_and_query(self):
        """ test exporting extremal values """
        schema = [("familyColumn", "col_A", ia.int32),
                  ("familyColumn", "col_B", ia.int64),
                  ("familyColumn", "col_3", ia.float32),
                  ("familyColumn", "Double", ia.float64),
                  ("familyColumn", "Text", str)]
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
        self.frame.export_to_hbase(tbl_name)

        hbase = ia.HBaseTable(tbl_name, schema)
        frame = ia.Frame(hbase)

        self.assertTrue(
            frame_utils.compare_frames(frame, self.frame, zip(frame.column_names, self.frame.column_names), almost_equal=True))

    def test_export_to_hbase_vector_error(self):
        """ export vector fails """
        schema = [("familyColumn", "col_A", ia.int32),
                  ("familyColumn", "col_B", ia.int64),
                  ("familyColumn", "col_3", ia.float32),
                  ("familyColumn", "Double", ia.float64),
                  ("familyColumn", "Text", str),
                  ("familyColumn", "vector", ia.vector(2))]
        tbl_name = common_utils.get_a_name("water_data")
        self.frame.add_columns(lambda row: [1.0, 2.0],
                               ("vector", ia.vector(2)))
        self.frame.export_to_hbase(tbl_name)

        hbase = ia.HBaseTable(tbl_name, schema)
        with self.assertRaises(ia.rest.command.CommandServerError):
		frame = ia.Frame(hbase)

    def test_export_to_hbase_table_overrides(self):
        """ exporting to existing table fails """
        schema = [("familyColumn", "col_A", ia.int32),
                  ("familyColumn", "col_B", ia.int64),
                  ("familyColumn", "col_3", ia.float32),
                  ("familyColumn", "Double", ia.float64),
                  ("familyColumn", "Text", str)]
        tbl_name = common_utils.get_a_name("water_data")

	self.frame.export_to_hbase(tbl_name)
        self.frame.append(self.frame)
	self.frame.export_to_hbase(tbl_name)

        hbase = ia.HBaseTable(tbl_name, schema)
        frame = ia.Frame(hbase)
        self.assertTrue(
            frame_utils.compare_frames(self.frame, frame, zip(self.frame.column_names, frame.column_names), almost_equal=True))


    def test_export_to_hbase_table_null_error(self):
        """ name can't be null """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_hbase,
                          None)


if __name__ == "__main__":
    unittest.main()
