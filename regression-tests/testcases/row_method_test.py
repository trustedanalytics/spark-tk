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
   Usage:  python2.7 row_method_test.py
   Test interface functionality of
     drop_rows
     filter
     append
     flatten_column
     drop_duplicates
     unicode contents
"""
__author__ = "WDW"
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "2014.12.31.001"


"""
Functionality tested (plan -- not entirely implemented):
  frame.drop_rows(predicate)
    -- and --
  frame.filter(predicate)
    no rows match
    some rows match
    all rows match
    predicate: lambda
    predicate: function
    predicate: simple expression
    predicate: compound expression
  error tests
    dropped frame

  append(float_frame)
    same # of columns
    more columns
    fewer columns
    column names all match
    column names all differ
    column names: some match, some differ
  error tests
    dropped frame: destination
    dropped frame: source

  flatten_column(column, delimiter=None)
    entries with one value
    entries with null value
    entries with multiple values
    comma delimiter
    other delimiter
  error tests
    multi-char delimiter
    non-existent column
    dropped frame

  drop_duplicates
    some duplicates
    no duplicates
    one duplicate for each line
    multiple duplicates for one line

  Unicode contents
    Load frame with Unicode strings

This test case replaces
  ManualTests
    DropRows01
    DropRows02
    Filter02
    FlattenTest
    FrameAppend-DeDup
    UnicodeOperations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class RowMethodTest(atk_test.ATKTestCase):

    def setUp(self):
        """Create standard and defunct frames for general use"""
        super(RowMethodTest, self).setUp()

        self.datafile_drop = "AddCol02.csv"
        self.schema_drop = [("col_A", ia.int32),
                            ("col_B", ia.int32),
                            ("col_C", ia.int32)]

        self.drop_frame = frame_utils.build_frame(
            self.datafile_drop, self.schema_drop, self.prefix)

        self.datafile_unicode = "unicode01.csv"
        self.schema_unicode = [("Text_A", str),
                               ("col_B", ia.int32),
                               ("col_C", ia.float32)]
        self.unicode_frame = frame_utils.build_frame(
            self.datafile_unicode, self.schema_unicode, self.prefix)

        datafile_flatten = "stackedNums.csv"
        schema_flatten = [("col_A", ia.int32),
                          ("col_B", str),
                          ("col_C", ia.int32)]

        self.flatten_frame = frame_utils.build_frame(
            datafile_flatten, schema_flatten, self.prefix)

        self.append_file_1col = "append_example_1col.csv"
        self.append_schema_1col = [("col_1", str)]

        self.append_file_2col = "append_example_2col.csv"
        self.append_schema_2col = [("col_1", str), ("col_qty", ia.int32)]

    def test_drop_lambda_simple(self):
        """
        drop_rows with a simple lambda expression
        """

        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Simple lambda expression"
        self.drop_frame.drop_rows(lambda row: row['col_B'] < 5)
        self.assertEqual(self.drop_frame.row_count, 23)

    def test_drop_lambda_compound(self):
        """
        drop_rows with a compound lambda expression
        """

        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Compound lambda expression"
        self.drop_frame.drop_rows(lambda row: row['col_A'] == 2 or
                                  row['col_C'] > 50)
        self.assertEqual(self.drop_frame.row_count, 17)

    def test_drop_function(self):
        """
        drop_rows with a function as predicate
        """

        def naturals_only(row):
            """filter/drop predicate"""
            return row['col_A'] <= 0 or row['col_B'] <= 0 or row['col_C'] <= 0

        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Function predicate"
        self.drop_frame.drop_rows(naturals_only)
        self.assertEqual(self.drop_frame.row_count, 27)

    def test_drop_all_none(self):
        """
        drop no  rows
        drop all rows
        """

        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Drop no rows"
        self.drop_frame.drop_rows(lambda row: row['col_A'] == 3.14159)
        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Drop all rows"
        self.drop_frame.drop_rows(lambda row: row['col_A'] > -1000)
        self.assertEqual(self.drop_frame.row_count, 0)

    def test_filter_lambda_simple(self):
        """
        filter with a simple lambda expression
        """

        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Simple lambda expression"
        self.drop_frame.filter(lambda row: row['col_B'] < 5)
        self.assertEqual(self.drop_frame.row_count, 10)

    def test_filter_lambda_compound(self):
        """
        filter with a compound lambda expression
        """
        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Compound lambda expression"
        self.drop_frame.filter(lambda row: row['col_A'] == 2 or
                               row['col_C'] > 50)
        self.assertEqual(self.drop_frame.row_count, 16)

    def test_filter_function(self):
        """
        filter with a function as predicate
        """

        def naturals_only(row):
            """filter/drop predicate"""
            return row['col_A'] <= 0 or row['col_B'] <= 0 or row['col_C'] <= 0

        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Function predicate"
        self.drop_frame.filter(naturals_only)
        self.assertEqual(self.drop_frame.row_count, 6)

    def test_filter_all_none(self):
        """
        filter no  rows
        filter all rows
        """
        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Drop no rows"
        self.drop_frame.filter(lambda row: row['col_A'] > -1000)
        self.assertEqual(self.drop_frame.row_count, 33)

        # print "Drop all rows"
        self.drop_frame.filter(lambda row: row['col_A'] == 3.14159)
        self.assertEqual(self.drop_frame.row_count, 0)

    def test_flatten_basic(self):
        """
        flatten comma-separated rows
        """

        self.assertEqual(self.flatten_frame.row_count, 11)

        # Verify that we can flatten rows with NaN and +/-Inf values.
        self.flatten_frame.flatten_columns('col_B')
        self.assertEqual(self.flatten_frame.row_count, 34)

    def test_flatten_extreme_vals(self):
        """
        flatten comma-separated rows
        """

        import numpy as np

        def make_it_interesting(row):
            if row["col_A"] == 1:
                return np.inf
            if row["col_A"] == 2:
                return -np.inf
            if row["col_A"] == 3:
                return np.nan
            return row["col_A"]

        self.assertEqual(self.flatten_frame.row_count, 11)

        # Verify that we can flatten rows with NaN and +/-Inf values.
        self.flatten_frame.add_columns(make_it_interesting,
                                       ("col_D", ia.float64))
        self.flatten_frame.flatten_columns('col_B')
        self.assertEqual(self.flatten_frame.row_count, 34)

    def test_drop_dup_basic(self):
        """
        Drop duplicates in a mixed-duplicate frame
        """

        self.assertEqual(self.drop_frame.row_count, 33)

        # Some rows have multiple duplicates; drop those
        # Some rows have no duplicates
        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        self.assertEqual(self.drop_frame.row_count, 21)

    def test_append_drop_dup(self):
        """
        Create a frame and strip out the duplicates.
        drop_duplicates on this frame (which has none).
        Append the frame to itself
            (exactly one duplicate per row) and drop again.
        """
        self.assertEqual(self.drop_frame.row_count, 33)

        # Drop initial duplicates
        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        orig_count = self.drop_frame.row_count
        self.assertEqual(self.drop_frame.row_count, 21)

        # Now there are no duplicates; drop
        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        self.assertEqual(self.drop_frame.row_count, orig_count)

        # Append frame to self; each is duplicated exactly once
        self.drop_frame.append(self.drop_frame)
        self.assertEqual(self.drop_frame.row_count, 2 * orig_count)

        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        self.assertEqual(self.drop_frame.row_count, orig_count)

    def test_append_example(self):
        """
        Execute the example in the Frame.append document:
        a one-column frame and a 2-column frame appended.
        """
        my_frame = frame_utils.build_frame(
            self.append_file_1col, self.append_schema_1col, self.prefix)
        your_frame = frame_utils.build_frame(
            self.append_file_2col, self.append_schema_2col, self.prefix)
        my_frame.append(your_frame)
        print my_frame.inspect(8)

        self.assertEqual(8, my_frame.row_count,
                         "Appending different frames has %d rows: expected 8."
                         % my_frame.row_count)
        append_take = my_frame.take(my_frame.row_count)
        frame_col1_list = [x[0] for x in append_take]
        dog_row = frame_col1_list.index("dog")
        self.assertIsNone(append_take[dog_row][-1],
                          "Expected None for dog-row extension; found %s" %
                          append_take[dog_row][-1])

    def test_row_unicode_add(self):
        """
        Test row functions with Unicode data
        """
        self.assertEqual(self.unicode_frame.row_count, 32)

        # print u"Test drop_rows; drop the row with
        #   %s (\\u24ce) at the end." % u'\u24CE'
        self.unicode_frame.drop_rows(lambda row:
                                     row.Text_A.endswith(u'\u24CE'))
        self.assertEqual(self.unicode_frame.row_count, 31)

        def insert_none(row):
            """Insert None at every 6th row"""
            new_val = row["col_B"]
            if row["col_B"] % 6 == 0:
                return None
            return new_val

        # print "test add_columns: copy col_B with a few 'None's inserted"
        self.unicode_frame.add_columns(insert_none,
                                       schema=("col_B2", ia.int32))
        self.assertIn("col_B2", self.unicode_frame.column_names)

        # print "Test filter: remove rows with inserted 'None' values"
        self.unicode_frame.filter(lambda row: row.col_B2 is not None)
        self.assertEqual(self.unicode_frame.row_count, 26)

    def test_row_unicode_drop(self):
        """
        Test row functions with Unicode data
        """
        self.assertEqual(self.unicode_frame.row_count, 32)

        def build_concat(row):
            """Create new column by concatenating several others:"""
            new_val = str(row["col_C"]) + " " + row.Text_A \
                + " - " + str(row["col_B"])
            if row["col_B"] % 7 == 0:
                new_val = None
            return new_val

        # print "Test building row values with Unicode"
        self.unicode_frame.add_columns(build_concat, schema=("Concat", str))
        self.assertIn("Concat", self.unicode_frame.column_names)

        # print "Test copy and projection (one column)"
        float_frame = self.unicode_frame.copy({"col_C": "float_C"})
        self.assertEqual(float_frame.row_count, 32)

        # print "Test drop_columns with Unicode row values"
        self.unicode_frame.drop_columns("Text_A")
        self.assertNotIn("Text_A", self.unicode_frame.column_names)
        self.assertEqual(float_frame.row_count, 32)


if __name__ == "__main__":
    unittest.main()
