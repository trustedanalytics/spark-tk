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
"""test jdbc under postgres"""
import unittest
import numpy as np

import trustedanalytics as ia

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from qalib import common_utils as cu
from qalib import config
from qalib import atk_test


class PostgresTest(atk_test.ATKTestCase):

    def setUp(self):
        """Initialize test data at beginning of test"""
        # Call parent set-up.
        super(PostgresTest, self).setUp()
        white_schema = [("col_A", ia.int32),
                        ("col_B", ia.int64),
                        ("col_3", ia.float32),
                        ("Double", ia.float64),
                        ("Text", str)]

        dataset_delimT = cu.get_file("delimTest1.tsv")
        self.frame = ia.Frame(ia.CsvFile(
            dataset_delimT, white_schema, delimiter='\t'))

    @unittest.skipIf(not config.on_tap, "Invalid on tap")
    def test_export_to_jdbc_postgres(self):
        """ test basic postgres funcitonality"""
        tbl_name = cu.get_a_name("atk_table")
        self.frame.drop_columns(['col_3', 'Double'])

        self.frame.export_to_jdbc(tbl_name, "postgres")
        frame = ia.Frame(ia.JdbcTable(tbl_name, "postgres"))

        frame_take = self.frame.take(self.frame.row_count)
        new_frame_take = frame.take(self.frame.row_count)

        self.assertItemsEqual(frame_take, new_frame_take)

    @unittest.skipIf(not config.on_tap, "Invalid on tap")
    def test_export_to_postgres_extreme(self):
        """ test exporting nan's to postgres"""
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
        tbl_name = cu.get_a_name("atk_table")

        self.frame.drop_columns(['col_3', 'Double'])

        self.frame.export_to_jdbc(tbl_name, "postgres")
        frame = ia.Frame(ia.JdbcTable(tbl_name, "postgres"))

        frame_take = self.frame.take(self.frame.row_count)
        new_frame_take = frame.take(self.frame.row_count)

        self.assertItemsEqual(frame_take, new_frame_take)

    @unittest.skipIf(not config.on_tap, "Invalid on tap")
    def test_export_to_postgres_vector_error(self):
        """ test_export_to_jdbc errors when handed a vector"""
        tbl_name = cu.get_a_name("water_data")
        self.frame.add_columns(lambda row: [1.0, 2.0],
                               ("vector", ia.vector(2)))
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_jdbc,
                          tbl_name, "postgres")

    @unittest.skipIf(not config.on_tap, "Invalid on tap")
    def test_export_to_postgres_table_exists(self):
        """exporting to an existing table errors"""
        tbl_name = cu.get_a_name("water_data")
        self.frame.export_to_jdbc(tbl_name, "postgres")
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_jdbc,
                          tbl_name, "postgres")

    @unittest.skipIf(not config.on_tap, "Invalid on tap")
    def test_export_to_postgres_null_table(self):
        """ writing to null table should error"""
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.export_to_jdbc,
                          None)

if __name__ == "__main__":
    unittest.main()
