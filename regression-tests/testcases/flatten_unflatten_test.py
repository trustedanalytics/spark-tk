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
Tests that flatten and unflatten work in multi-column mode
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.10.13"


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ta

from qalib import frame_utils
from qalib import atk_test


class FlattenUnflatten(atk_test.ATKTestCase):

    def setUp(self):
        """Create standard and defunct frames for general use"""
        super(FlattenUnflatten, self).setUp()
        self.default_str = None         # Placeholder for missing string value

    def test_flatten_unflatten_basic(self):
        """ test for unflatten comma-separated rows """

        # file contains 11 rows
        datafile_flatten = "stackedNums.csv"
        schema_flatten = [("col_A", ta.int32),
                          ("col_B", str),
                          ("col_C", ta.int32)]
        self.frame = frame_utils.build_frame(
            datafile_flatten, schema_flatten, self.prefix)

        original_copy = self.frame.download()
        row_count = self.frame.row_count
        self.assertEqual(row_count, 11)

        # flatten data
        self.frame.flatten_columns('col_B')
        flat_copy = self.frame.download()
        flat_row_count = self.frame.row_count

        # lets call unflatten
        self.frame.unflatten_columns(['col_A'])
        unflat_copy = self.frame.download()
        # confirm number of lines equal to original file count
        self.assertEqual(self.frame.row_count, row_count)
        # confirm original copy and unflatten copy are same
        diff = set(unflat_copy.keys()) - set(original_copy.keys())
        self.assertEqual(len(diff), 0)

        # flatten this frame, compare with previous flatten result
        # no changes in data
        self.frame.flatten_columns('col_B')
        reflat_copy = self.frame.download()
        # confirm number of lines equal to original file
        self.assertEqual(self.frame.row_count, flat_row_count)
        # confirm both reflat copy and flat copy have same values
        diff = set(reflat_copy.keys()) - set(flat_copy.keys())
        self.assertEqual(len(diff), 0)

    def test_flatten_mult_simple(self):
        """Test on multiple columns"""
        block_data = [
            [[4, 3, 2, 1],
             "Calling cards,"
             "French toast,"
             "Turtle necks,"
             "Partridge in a Parody"],
            [[8, 7, 6, 5],
             "Maids a-milking,"
             "Swans a-swimming,"
             "Geese a-laying,"
             "Gold rings"],
            [[12, 11, 10, 9],
             "Drummers drumming,"
             "Lords a-leaping,"
             "Pipers piping,"
             "Ladies dancing"]
        ]

        block_schema = [("day", ta.vector(4)), ("gift", str)]

        expected_take = [
            [12, "Drummers drumming"],
            [11, "Lords a-leaping"],
            [10, "Pipers piping"],
            [9, "Ladies dancing"],
            [8, "Maids a-milking"],
            [7, "Swans a-swimming"],
            [6, "Geese a-laying"],
            [5, "Gold rings"],
            [4, "Calling cards"],
            [3, "French toast"],
            [2, "Turtle necks"],
            [1, "Partridge in a Parody"],
        ]

        frame = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        frame.flatten_columns(["day", "gift"])
        frame.sort("day", False)
        print frame.inspect(frame.row_count)
        frame_take = frame.take(frame.row_count)
        self.assertEqual(frame_take, expected_take)

    def test_flatten_vary_len(self):
        """Test on vector against longer string list"""
        block_data = [
            ["1,2,3", "a,b,c,d"]
        ]

        expected_take = [
            [1, 'a'],
            [2, 'b'],
            [3, 'c'],
            [None, 'd']
        ]
        # Vector for col1 fails; DPNG-2964.
        # String is okay.
        block_schema = [("col1", ta.vector(3)), ("col2", str)]

        frame = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        print frame.inspect()
        frame = frame.copy()

        frame.flatten_columns(frame.column_names)
        print frame.inspect()
        frame_take = frame.take(frame.row_count)
        self.assertEqual(frame_take, expected_take)

    def test_flatten_delim_variety(self):
        """Test on multiple columns"""
        block_data = [
            ["a b c", "d/e/f", "10,18,20"],
            ["g h", "i/j", "4,5"],
            ["k l", "m/n", "13"],
            ["o", "p/q", "7,25,6"]
        ]
        block_schema = [("col1", str), ("col2", str), ("col3", str)]

        frame = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        self.assertRaises(ta.rest.command.CommandServerError,
                          frame.flatten_columns,
                          ["col1", "col2", "col3"], [' ', '/'])

    def test_flatten_delim_same(self):
        """Test on multiple columns"""
        block_data = [
            ["a=b=c", "d=e=f", "10=18=20"],
            ["g=h", "i=j", "4=5"],
            ["k=l", "m=n", "13,13"],    # Check that comma is not a delimiter
            ["o", "p=q", "7=25=6"]
        ]
        block_schema = [("col1", str), ("col2", str), ("col3", str)]

        expected_flat = [
            [self.default_str, self.default_str, "6"],
            [self.default_str, "q", "25"],
            ["a", "d", "10"],
            ["b", "e", "18"],
            ["c", "f", "20"],
            ["g", "i", "4"],
            ["h", "j", "5"],
            ["k", "m", "13,13"],
            ["l", "n", self.default_str],
            ["o", "p", "7"]
        ]

        frame = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        print frame.inspect()
        frame.flatten_columns(["col1", "col2", "col3"], ['='])
        frame.sort(["col1", "col2", "col3"])
        print frame.inspect()
        actual_flat = frame.take(frame.row_count)
        print actual_flat
        self.assertEqual(expected_flat, actual_flat)

    def test_flatten_mult_example(self):
        """ Test on multiple columns, using examples from Nate's specification.
        """
        block_data = [
            ["a,b,c", "d,e,f", "10,18,20"],
            ["g,h", "i,j", "4,5"],
            ["k,l", "m,n", "13"],
            ["o", "p,q", "7,25,6"]
        ]
        block_schema = [("col1", str), ("col2", str), ("col3", str)]

        expected_flat = [
            [self.default_str, self.default_str, "6"],
            [self.default_str, "q", "25"],
            ["a", "d", "10"],
            ["b", "e", "18"],
            ["c", "f", "20"],
            ["g", "i", "4"],
            ["h", "j", "5"],
            ["k", "m", "13"],
            ["l", "n", self.default_str],
            ["o", "p", "7"]
        ]

        frame1 = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        print frame1.inspect()
        frame = frame1.copy()

        frame1.flatten_columns("col3")
        print frame1.inspect(frame1.row_count)
        self.assertEqual(9, frame1.row_count)

        frame.flatten_columns(["col1", "col2", "col3"])
        frame.sort(["col1", "col2", "col3"])
        print frame.inspect()
        actual_flat = frame.take(frame.row_count)
        print actual_flat
        self.assertEqual(expected_flat, actual_flat)

        # Unflatten should do nothing: there are no common elements.
        frame.unflatten_columns(["col1", "col2", "col3"])
        frame.sort(["col1", "col2", "col3"])
        print frame.inspect()
        actual_unflat = frame.take(frame.row_count)
        print actual_unflat
        self.assertEqual(expected_flat, actual_unflat)


if __name__ == "__main__":
    unittest.main()
