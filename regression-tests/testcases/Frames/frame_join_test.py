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
    Usage:  python2.7 frame_join_test.py
    Test column naming and quantity of joins.
"""
# Features
# Positive:
#   Simple join succeeds
#   Column name collisions are properly handled
#   New column names are reasonable
#   Any supported data type can be a key column
#   Frame name: default | specified
#
# Degenerate:
#   No rows in left / right / both frames
#   No rows in common for each join type
#
# Negative:
#   Key columns of incompatible types
#   Key column does not exist
#   Join type does not exist

__author__ = 'Prune Wickart, Lewis Coates'
__credits__ = ["Prune Wickart", "Lewis Coates"]
__version__ = "2015.08.28"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class JoinTest(atk_test.ATKTestCase):

    def setUp(self):
        """Set up frames to be build """
        super(JoinTest, self).setUp()
        self.datafile = "employees.csv"
        self.schema = [("userId", str),
                       ("name", str),
                       ("age", str),
                       ("dept", str),
                       ("manager", str),
                       ("years", str)]

        self.example_classification_data = "classification_example.csv"
        self.example_classification_schema = [("a", str),
                                              ("b", ia.int32),
                                              ("labels", ia.int32),
                                              ("predictions", ia.int32)]

        self.example_data = "example_count_v2.csv"
        self.example_schema = [("idnum", ia.int32)]

    def test_name_collision(self):
        """
        Exercise a simple join operation.
        Repeat many times to stress the name-collision resolution.
        """
        # There are no assert statements in this:
        #   completing without error is success.
        # Drop all columns except the age
        old_frame = frame_utils.build_frame(
            self.datafile, self.schema, self.prefix)
        old_frame.drop_columns(["userId", "name", "dept", "manager"])
        base = old_frame.copy()

        # TRIB-4248 repeated suffix makes poor names
        # TRIB-4236 exception on name collision at 4-join
        limit = 6
        new_frame = None
        for i in range(2, limit):
            new_frame = old_frame.join(base,
                                       left_on="years",
                                       right_on="years",
                                       how="inner")
            ia.drop_frames(old_frame)
            old_frame = new_frame
            print "%d rows in %d-join" % (new_frame.row_count, i)
            print new_frame.inspect(5)

        if new_frame:
            ia.drop_frames(new_frame)

    def test_type_int32(self):
        """
        test join works on int32
        test conversion to PANDAS frame
        """
        frame1 = frame_utils.build_frame(
            self.example_classification_data,
            self.example_classification_schema,
            skip_header_lines=1)

        frame2 = frame_utils.build_frame(
            self.example_classification_data,
            self.example_classification_schema,
            skip_header_lines=1)

        joined_frame = frame1.join(frame2, "b")
        joined_df = joined_frame.take(joined_frame.row_count).sort()

        baseline_df = [["red  ", 1, 0, 0, "red  ", 0, 0],
                       ["red  ", 1, 0, 0, "blue ", 0, 0],
                       ["blue ", 3, 1, 0, "blue ", 1, 0],
                       ["blue ", 1, 0, 0, "red  ", 0, 0],
                       ["blue ", 1, 0, 0, "blue ", 0, 0],
                       ["green", 0, 1, 1, "green", 1, 1]].sort()
        self.assertEqual(baseline_df, joined_df)

    def test_empty_partner(self):
        """
        Join with an empty frame: left, right, and both.
        Result should be the other frame.
        Also test explicit frame name.
        """

        block_data = [
            [0, "sifuri"],
            [1, "moja"],
            [2, "mbili"],
            [3, "tatu"],
            [4, "nne"],
            [5, "tano"]
        ]
        schema = [("idnum", ia.int32), ("count", str)]

        empty_frame = frame_utils.build_frame(
            [], schema, self.prefix, file_format="list")
        print "EMPTY:\n", empty_frame.inspect()

        # other_frame = frame_utils.build_frame(
        #    block_data, schema, file_format='data')  # Future enhancement
        other_frame = frame_utils.build_frame(block_data, schema, self.prefix, file_format="list")
        print "OTHER:\n", other_frame.inspect()

        # left frame empty
        join_frame = empty_frame.join(other_frame, "idnum", how="outer")
        print "LEFT EMPTY:\n", join_frame.inspect()
        self.assertEquals(len(block_data), join_frame.row_count)

        # right frame empty
        join_frame = other_frame.join(
            empty_frame, "idnum", how="outer",
            name=common_utils.get_a_name("right_empty"))
        print "RIGHT EMPTY:\n", join_frame.inspect()
        self.assertEquals(len(block_data), join_frame.row_count)
        self.assertIn("right_empty", join_frame.name)

        # both frames empty
        join_frame = empty_frame.join(empty_frame, "idnum", how="outer")
        print "BOTH EMPTY:\n", join_frame.inspect()
        self.assertEquals(0, join_frame.row_count)
        print "frame name", join_frame.name
        self.assertIsNone(join_frame.name)

    def test_disjoint(self):
        """
        Join with an empty frame: left, right, and both.
        Result should be the other frame.
        """

        block_data = [
            [0, "sifuri"],
            [1, "moja"],
            [2, "mbili"],
            [3, "tatu"],
            [4, "nne"],
            [5, "tano"]
        ]
        right_data = [
            [6, "sita"],
            [7, "saba"],
            [8, "nane"],
            [9, "tisa"],
            [10, "kumi"]
        ]
        schema = [("idnum", ia.int32), ("count", str)]

        block_data_len = len(block_data)
        right_data_len = len(right_data)
        total_len = block_data_len + right_data_len

        left_frame = frame_utils.build_frame(
            block_data, schema, self.prefix, file_format="list")
        print left_frame.inspect()

        right_frame = frame_utils.build_frame(
            right_data, schema, self.prefix, file_format="list")
        print right_frame.inspect()

        # outer join
        join_frame = left_frame.join(right_frame, "idnum", how="outer")
        print join_frame.inspect(20)
        self.assertEquals(total_len, join_frame.row_count)

        # left join
        join_frame = left_frame.join(right_frame, "idnum", how="left")
        print join_frame.inspect(20)
        self.assertEquals(block_data_len, join_frame.row_count)

        # right join
        join_frame = left_frame.join(right_frame, "idnum", how="right")
        print join_frame.inspect(20)
        self.assertEquals(right_data_len, join_frame.row_count)

        # inner join
        join_frame = left_frame.join(right_frame, "idnum", how="inner")
        print join_frame.inspect(20)
        self.assertEquals(0, join_frame.row_count)

    @unittest.skip("Compatibility check is changing")
    def test_type_compatible(self):
        """
        Check compatibility among the numeric types
        """

        block_data = [
            [0, "sifuri"],
            [1, "moja"],
            [2, "mbili"],
            [3, "tatu"],
            [4, "nne"],
            [5, "tano"]
        ]

        def complete_data_left(row):
            return block_data[int(row.idnum)]

        # Create a frame indexed by each of the numeric types
        int32_frame = frame_utils.build_frame(
            block_data,
            [("idnum", ia.int32), ("count", str)],
            self.prefix, file_format="list")
        int64_frame = frame_utils.build_frame(
            block_data,
            [("idnum", ia.int64), ("count", str)],
            self.prefix, file_format="list")
        flt32_frame = frame_utils.build_frame(
            block_data,
            [("idnum", ia.float32), ("count", str)],
            self.prefix, file_format="list")
        flt64_frame = frame_utils.build_frame(
            block_data,
            [("idnum", ia.float64), ("count", str)],
            self.prefix, file_format="list")

        print int32_frame.inspect
        print int64_frame.inspect
        print flt32_frame.inspect
        print flt64_frame.inspect

        # Try each join combination;
        #   make sure each type gets to be left & right at least once.
        # float32 & float64,
        #   int32 & int64 are compatible pairs.
        join_i32_i64 = int32_frame.join(int64_frame, "idnum")
        print join_i32_i64.inspect()
        self.assertEquals(int32_frame.row_count, join_i32_i64.row_count)
        join_i32_f32 = int32_frame.join(flt32_frame, "idnum")
        print join_i32_f32.inspect()
        self.assertEquals(int32_frame.row_count, join_i32_f32.row_count)

        # int and float are not compatible with each other.
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            flt32_frame.join(int32_frame, "idnum")
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            flt32_frame.join(int64_frame, "idnum")
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            flt64_frame.join(int32_frame, "idnum")
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            flt64_frame.join(int64_frame, "idnum")

    def test_type_fail(self):
        """
        test join fails on mismatched type
        """
        frame1 = frame_utils.build_frame(
            self.example_classification_data,
            self.example_classification_schema,
            self.prefix,
            skip_header_lines=1)

        example_classification_schema = [("a", str),
                                         ("b", str),
                                         ("labels", ia.int32),
                                         ("predictions", ia.int32)]

        frame2 = frame_utils.build_frame(
            self.example_classification_data,
            example_classification_schema,
            skip_header_lines=1)

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            frame1.join(frame2, "b")

    def test_join_how_fail(self):
        """
        test join faults on unsupported join type
        """
        frame1 = frame_utils.build_frame(
            self.example_classification_data,
            self.example_classification_schema,
            self.prefix,
            skip_header_lines=1)

        frame2 = frame1.copy()

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            frame1.join(frame2, "a", how="full")

    def test_join_no_column(self):
        """
        test join faults on unsupported join type
        """
        frame1 = frame_utils.build_frame(
            self.example_classification_data,
            self.example_classification_schema,
            skip_header_lines=1)

        frame2 = frame1.copy()

        with(self.assertRaises(ia.rest.command.CommandServerError)):
            frame1.join(frame2, left_on="no_such_column", right_on="a")
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            frame1.join(frame2, left_on="a", right_on="no_such_column")
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            frame1.join(
                frame2, left_on="no_such_column", right_on="no_such_column")
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            frame1.join(frame2, left_on="a", right_on="")


if __name__ == "__main__":
    unittest.main()
