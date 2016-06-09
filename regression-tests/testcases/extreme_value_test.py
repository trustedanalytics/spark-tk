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
    Usage:  python2.7 extreme_value_test.py
    Exercise NaN and Inf values in various contexts.
"""
__author__ = 'Prune Wickart'
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "2015.01.05"

# Test various functions with extreme values, including
#   NaN
#   Inf, -Inf
#   greatest and least magnitude numbers ...
#     positive & negative ...
#     int & float ...
#     32-bit and 64-bit
#
# Test with frame methods that manipulate data
# So far, this includes
#   add_columns
#   drop_columns
#   drop_rows
#   inspect
#   take
#   copy
#   column_mode
#   column_summary_statistics
#
# This test suite replaces manual test cases
#   2245-min* (3 files)
#   InfAndNan
#   mode_stats
#   ProjectionTest_debug1
#   summary_statistics
#   summary_statistics_edges
#   TableCopy

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia
import numpy as np

from qalib import frame_utils
from qalib import atk_test


class ExtremeValueTest(atk_test.ATKTestCase):

    def setUp(self):
        super(ExtremeValueTest, self).setUp()

        self.data_maxmin32 = 'BigAndTinyFloat32s.csv'
        self.schema_maxmin32 = (["col_A", ia.float32], ["col_B", ia.float32])

        self.data_maxmin64 = 'BigAndTinyFloat64s.csv'
        self.schema_maxmin64 = (["col_A", ia.float64], ["col_B", ia.float64])

        self.data_stat = "mode_stats.tsv"
        self.schema_stat = [("weight", ia.float64), ("item", str)]

        self.data_proj = "typesTest3.csv"
        self.schema_proj = [("col_A", ia.int32),
                            ("col_B", ia.int64),
                            ("col_C", ia.float32),
                            ("Double", ia.float64),
                            ("Text", str)]

        self.data_mode = "mode_stats2.csv"
        self.schema_mode = [("Item", ia.int32), ("Weight", ia.int32)]

        self.data_corner = "SummaryStats2.csv"
        self.schema_corner = [("Item", ia.float64), ("Factors", ia.float64)]

    def test_extreme_maxmin32(self):
        """
        Test extreme large and small magnitudes on 32-bit floats.
        Replaces test case InfsAndNaNCheck
        """
        extreme32 = frame_utils.build_frame(
            self.data_maxmin32, self.schema_maxmin32, skip_header_lines=1)
        self.assertEqual(extreme32.row_count, 16)

        # print "Here's what we loaded:\n %s" % extreme32.inspect(30)

        extreme32.add_columns(lambda row: row["col_A"] * 2,
                              ["twice", ia.float32])
        extreme32.add_columns(lambda row: np.sqrt(-9),
                              ["neg_root", ia.float32])
        extreme32.sort(["col_A", "col_B"])
        print extreme32.inspect(10)
        extake = extreme32.take(10)
        self.assertEqual(extake[6][2], 2 * extake[6][0])
        self.assertIsNone(extake[0][2])
        self.assertIsNone(extake[0][3])
        self.assertIsNone(extake[6][3])
        ia.drop_frames(extreme32)

    def test_extreme_maxmin64(self):
        """
        Test extreme large and small magnitudes on 64-bit floats.
        Replaces test case InfsAndNaNCheck
        """
        extreme64 = frame_utils.build_frame(
            self.data_maxmin64, self.schema_maxmin64, skip_header_lines=1)
        self.assertEqual(extreme64.row_count, 16)

        # print "Here's what we loaded:\n %s" % extreme64.inspect(30)
        extreme64.add_columns(lambda row: row["col_A"] * 2,
                              ["twice", ia.float64])
        extreme64.add_columns(lambda row: np.sqrt(-9),
                              ["neg_root", ia.float64])
        extreme64.sort(["col_A", "col_B"])
        print extreme64.inspect(10)
        extake = extreme64.take(10)
        self.assertEqual(extake[6][2], 2 * extake[6][0])
        self.assertIsNone(extake[0][2])
        self.assertIsNone(extake[0][3])
        self.assertIsNone(extake[6][3])
        ia.drop_frames(extreme64)

    def test_extreme_colmode(self):
        """
        Insert NaN and +/-Inf for weights; does it breaks mode computations?
        """
        def add_extremes(row):
            new_val = {
                "Charizard": np.inf,
                "Squirtle": -np.inf,
                "Wartortle": np.nan
            }
            try:
                return new_val[row["item"]]
            except KeyError:
                return row["weight"]

        stat_frame = frame_utils.build_frame(
            self.data_stat, self.schema_stat, delimit=u'\t')
        stat_frame.add_columns(add_extremes, ("weight2", ia.float64))
        stats = stat_frame.column_mode("item", "weight2",
                                       max_modes_returned=50)

        # print stats
        # for k, v in stats.iteritems():
        #     print "%s \t %s" % (k, v)
        self.assertEqual(stats["mode_count"], 5)
        self.assertEqual(stats["total_weight"], 1743)
        self.assertIn("Scrafty", stats["modes"])

        ia.drop_frames(stat_frame)

    def test_extreme_projection(self):
        """
        Test projection including Inf / NaN data
        """

        # Create the frame and give it a name as master
        master = frame_utils.build_frame(self.data_proj, self.schema_proj)
        print "inspect", master.inspect(7)
        self.assertEqual(master.row_count, 7)

        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {
                123456: np.inf,
                777: -np.inf,
                4321: np.nan
            }
            try:
                return new_val[row["col_A"]]
            except KeyError:
                return row["col_A"]

        master.add_columns(add_extremes, ("col_D", ia.float64))
        master_sort = sorted(master.take(master.row_count))

        print "Create projections"
        proj_3col = ia.Frame(master[['col_D', 'Double', 'Text']])
        self.assertEqual(proj_3col.row_count, master.row_count)
        self.assertEqual(len(proj_3col.column_names), 3)

        proj_1col = master.copy({'col_A': 'extremes'})
        self.assertEqual(proj_1col.row_count, master.row_count)
        self.assertEqual(len(proj_1col.column_names), 1)

        print "Verify master is unchanged"
        self.assertEqual(master_sort, sorted(master.take(master.row_count)))

        # Clean up
        ia.drop_frames([master, proj_3col, proj_1col])
        fail_frame = master.get_error_frame()
        if fail_frame:
            ia.drop_frames(fail_frame)

    def test_extreme_col_summary(self):
        """
        Test column_summary_stats with Inf / NaN data
        """
        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {
                20: np.inf,
                30: -np.inf,
                40: np.nan
            }
            try:
                return new_val[row.Item]
            except KeyError:
                return row.Item

        stat_frame = frame_utils.build_frame(self.data_mode, self.schema_mode)

        # Create new column where only values 10 and 50 are valid;
        #   Substitute that for the original
        stat_frame.add_columns(add_extremes, ("Item2", ia.float64))
        stat_frame.drop_columns("Item")
        stat_frame.rename_columns({"Item2": "Item"})

        print(stat_frame.inspect(5))    # See if the frame is intact
        stats = stat_frame.column_summary_statistics("Item", "Weight", True)
        print stats

        # for k, v in stats.iteritems():
        #     print "%s \t %s" % (k, v)
        self.assertEqual(stats["mean"], 30)
        self.assertEqual(stats["good_row_count"], 20)
        self.assertAlmostEqual(stats["mean_confidence_upper"], 34.3826932359)
        ia.drop_frames(stat_frame)

    def test_extreme_col_summary_corner(self):
        """
        Test column_summary_stats with very large values and odd cases.
          big float
          no rows
        """
        def add_extremes(row):
            bf = ia.float64(2) ** 1021
            new_val = {
                600: 30,
                1: 0,
                2: bf * 2,
                3: ia.float64(2) ** 1023,
                3.1: 5.4
            }
            try:
                return new_val[row.Factors]
            except KeyError:
                return row.Factors

        # Now we create the frame
        stat_frame = frame_utils.build_frame(
            self.data_corner, self.schema_corner)
        stat_frame.add_columns(add_extremes, schema=["Weight", ia.float64])
        stat_frame.drop_columns("Factors")
        stats = stat_frame.column_summary_statistics("Item", "Weight")

        # for k, v in stats.iteritems():
        #     print "%s \t %s" % (k, v)
        self.assertEqual(stats["mean"], 1624.6)
        self.assertEqual(stats["good_row_count"], 56)
        self.assertAlmostEqual(stats["variance"], 563700.64)

        print "Drop all rows."
        stat_frame.drop_rows(lambda row: row)  # Just drops all rows
        stats = stat_frame.column_summary_statistics("Item", "Weight")

        # for k, v in stats2.iteritems():
        #     print "%s \t %s" % (k, v)
        self.assertIsNone(stats["maximum"])
        self.assertEqual(stats["good_row_count"], 0)
        self.assertEqual(stats["geometric_mean"], 1.0)

        fails = stat_frame.get_error_frame()
        if fails:
            ia.drop_frames(fails)
        ia.drop_frames(stat_frame)

    def test_extreme_copy(self):
        """
        Test copy with Inf / NaN data
        """
        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {
                700: np.inf,
                701: -np.inf,
                702: np.nan
            }
            try:
                return new_val[row["col_A"]]
            except KeyError:
                return row["col_A"]

        frame = frame_utils.build_frame(self.data_proj, self.schema_proj)
        frame.add_columns(add_extremes, ("col_D", ia.float64))
        frame.sort("col_A")
        frame_sort = frame.take(frame.row_count)

        frame_copy = frame.copy()
        frame_copy.sort("col_A")
        copy_sort = frame_copy.take(frame_copy.row_count)
        self.assertEqual(frame.column_names, frame_copy.column_names)
        self.assertEqual(frame_sort, copy_sort)

        # Clean up
        fails = frame.get_error_frame()
        if fails:
            ia.drop_frames(fails)
        ia.drop_frames([frame, frame_copy])


if __name__ == "__main__":
    unittest.main()
