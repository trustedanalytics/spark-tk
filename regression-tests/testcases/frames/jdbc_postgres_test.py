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
