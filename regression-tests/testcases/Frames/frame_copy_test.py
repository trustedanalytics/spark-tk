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
   Usage:  python2.7 frame_copy_test.py
   Test interface functionality of
        frame.copy
"""
__author__ = "WDW"
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "2014.11.06"

# Functionality tested:
#   ia.Frame(<old>, new_name)
#     simple copy
#     omit new name
#     new name = None
#     new name = ""
#     empty old frame
#     error: dropped old frame
#   frame.copy(columns=None)
#     simple copy (default arguments)
#     columns = None (explicit)
#     all columns, original order
#     all columns, change order
#     subset of columns, change order
#     rename columns
#     empty old frame
#     error: dropped old frame
#
# This test case replaces
#   harnesses
#     <none>
#   TUD
#     TableCopy

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class FrameCopyTest(atk_test.ATKTestCase):

    def setUp(self):
        """Create standard frames"""
        super(FrameCopyTest, self).setUp()

        dataset = "typesTest3.csv"
        dataset_unique = "uniqueVal-rand2M-10KRecs.csv"
        dataset_unique_schema = [('datapoint', str), ('randval', int)]
        # Create a frame using unique sequential values and random numbers
        self.frame_unique = frame_utils.build_frame(
            dataset_unique, dataset_unique_schema, self.prefix)

        schema = [("col_A", ia.int32),
                  ("col_B", ia.int64),
                  ("col_C", ia.float32),
                  ("Double", ia.float64),
                  ("Text", str)]

        self.frame = frame_utils.build_frame(dataset, schema, self.prefix)
        self.orig_sort = sorted(self.frame.take(self.frame.row_count))
        self.orig_sort_2cols = sorted(self.frame.take(self.frame.row_count,
                                      columns=("col_B", "Text")))

        data_pred = 'uniqueVal-rand2M-10KRecs.csv'
        schema_pred = [('datapoint', str), ('randval', int)]
        self.frame_pred = frame_utils.build_frame(data_pred, schema_pred)

    def test_frame_copy_default(self):
        """
        Validate explicit copy with frame.copy()
        """

        print "Copy default"
        copy_frame = self.frame.copy()
        print "copy:", copy_frame
        # copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        copy_take = copy_frame.take(copy_frame.row_count)
        print "copy_take:", copy_take
        copy_sort = sorted(copy_take)
        print "ORIGINAL:", self.orig_sort
        print "COPY    :", copy_sort
        self.assertEqual(self.orig_sort,
                         copy_sort,
                         ("Default copy not identical to original. https://"
                          "jira01.devtools.intel.com/browse/TRIB-4220"))
        ia.drop_frames(copy_frame)

    def test_copy_column_default(self):
        """Test the default column copy functionality."""
        print "Copy with explicit column default"
        copy_frame = self.frame.copy(columns=None)
        copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        self.assertEqual(self.orig_sort,
                         copy_sort,
                         "Copy 'columns=None' not identical to original.")
        ia.drop_frames(copy_frame)

    def test_copy_all_columns(self):
        """Test the copy all column functionality."""
        print "Copy with all columns cited"
        column_list = ["col_A", "col_B", "col_C", "Double", "Text"]
        copy_frame = self.frame.copy(columns=column_list)

        copy_sort = sorted(copy_frame.take(copy_frame.row_count))

        self.assertEqual(
            self.orig_sort, copy_sort,
            ("Straight copy not identical to original; "
             "https://jira01.devtools.intel.com/browse/TRIB-4255"))

        ia.drop_frames(copy_frame)

    def test_copy_all_renamed(self):
        """Test copying while renaming all columns."""
        rename_dict = {
            "col_A": "alpha",
            "col_B": "bravo",
            "col_C": "charlie",
            "Double": "delta",
            "Text": "echo",
        }
        print "Copy with all columns renamed"
        copy_frame = self.frame.copy(columns=rename_dict)
        copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        print "original:\n", self.frame.inspect()
        print "copy\n:", copy_frame.inspect()
        self.assertEqual(self.orig_sort, copy_sort,
                         "TRIB-4255: Renamed copy not identical to original.")
        ia.drop_frames(copy_frame)

    def test_copy_where(self):
        """Test copy with column rename and 'where' function"""

        # column_list = ("col_A", "col_B", "col_C", "Double", "Text")

        rename_dict = {
            "col_A": "alpha",
            "col_B": "beta",
            "Text": "epsilon"
        }
        copy_frame = self.frame.copy(columns=rename_dict,
                                     where=lambda row: row.Double > 0)

        self.assertEqual(copy_frame.row_count+2, self.frame.row_count)
        self.assertIn("alpha", copy_frame.column_names)
        self.assertNotIn("col_B", copy_frame.column_names)
        self.assertNotIn("Double", copy_frame.column_names)

    def test_take_and_inspect(self):
        """Test take and inspect pull frames in the same order"""
        # TRIB-4126: take and inspect don't pull columns in the same order
        print "Copy with selected columns"
        col_choice = ("col_B", "Text")
        copy_frame = self.frame.copy(columns=col_choice)
        copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        # @unittest.expectedFailure
        self.assertEqual(self.orig_sort_2cols,
                         copy_sort,
                         ("2-col copy not identical to original. https://"
                          "jira01.devtools.intel.com/browse/TRIB-4126"))
        ia.drop_frames(copy_frame)

    def test_frame_copy_simple(self):
        """
        Copy implicitly, using an existing frame as the source
        new_frame = ia.Frame(old_frame, name="new_name")
        """
        print "Simple copy, no new name"
        copy_frame = ia.Frame(self.frame)
        # copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        print "copy:", copy_frame
        copy_take = copy_frame.take(copy_frame.row_count)
        print "copy_take:", copy_take
        copy_sort = sorted(copy_take)
        print "ORIGINAL:", self.orig_sort
        print "COPY    :", copy_sort
        self.assertEqual(self.orig_sort,
                         copy_sort,
                         ("Copy not identical to original: "
                          "name defaulted. https://"
                          "jira01.devtools.intel.com/browse/TRIB-4220"))
        ia.drop_frames(copy_frame)

    def test_copy_name_none(self):
        """Test copy with an explicit name of None"""
        print "Simple copy, new name is an explicit 'None'"
        copy_frame = ia.Frame(self.frame, name=None)
        copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        self.assertEqual(self.orig_sort,
                         copy_sort,
                         "Copy not identical to original: explicit None name.")
        ia.drop_frames(copy_frame)

    def test_copy_new_name(self):
        """Test copying with a new name"""
        print "Simple copy, new name given"
        copy_frame = ia.Frame(self.frame)
        copy_sort = sorted(copy_frame.take(copy_frame.row_count))
        self.assertEqual(self.orig_sort,
                         copy_sort,
                         "Copy not identical to original: new name given.")
        ia.drop_frames(copy_frame)

    def test_old_empty_frame(self):
        """Test the original of a copied frame is empty"""
        # print "Old frame is empty"      # TRIB-3968
        copy_frame = ia.Frame()
        self.assertEqual(copy_frame.row_count, 0,
                         "Copy of empty frame has rows.")
        self.assertEqual(len(copy_frame.column_names), 0,
                         "Copy of empty frame has columns.")
        ia.drop_frames(copy_frame)

    # @unittest.skip("TRIB-4508")
    # currently forced failure by above error, annotate if not
    # going to be fixed
    def test_copy_empty_name(self):
        """Copying with an empty name should error"""
        # print "Simple copy, new name is empty"
        self.assertRaises(ia.rest.command.CommandServerError,
                          ia.Frame, self.frame, name='')

    def test_predicated_copy(self):
        """
        Test copy ... where & lambda functionality.
        simple lambda
        where = lambda
        lambda with column selection
        lambda with compound condition
        """
        origin_frame = self.frame_unique
        # Count of total rows in frame
        origin_count = origin_frame.row_count
        # Count random values ending in 7
        sevens_count = origin_frame.count(lambda row: row.randval % 10 == 7)
        # Count random values ending in 2
        twos_count = origin_frame.count(lambda row: row.randval % 10 == 2)

        # Add a new string composed of the 2 extant columns for verification
        # purposes
        self.frame_pred.add_columns(
            lambda row: row.datapoint + str(row.randval), ("combo", str))

        # print self.frame_pred.inspect(50) <-- for debug

        # Below we copy all columns of all rows where randval is an even number

        # Should get about half the frame
        half = self.frame_pred.copy(where=lambda row: row.randval % 2 == 0)
        # Assert that the new frame is about half the length of the
        # original frame
        self.assertAlmostEqual(
            ia.float32(half.row_count) / ia.float32(origin_count),
            0.5, delta=.01)
        ia.drop_frames(half)

        # Creates new frame by copying column 'randval' where the number ends
        # in 7
        sevens = self.frame_pred.copy("randval",
                                      lambda row: row.randval % 10 == 7)

        self.assertEqual(sevens.row_count, sevens_count)

        self.assertEqual(sevens.count(lambda row: str(row.randval)[-1] == "7"),
                         sevens_count)
        self.assertEqual(sevens.count(lambda row: str(row.randval)[-1] != "7"),
                         0)

        def check_sevens(row):
            return str(row.randval)[-1] != "7"

        # Should drop all rows. If not, we have a problem
        sevens.filter(check_sevens)

        # Create a new frame from the 'datapoint' and 'combo' columns where
        # 'randval' ends in 2
        twos = self.frame_pred.copy(("datapoint", "combo"),
                                    lambda row: row.randval % 10 == 2)
        # Should match number of rows ending in 2 in original frame
        self.assertEqual(twos.row_count, twos_count)

        # def check_twos(row):
        #     return str(row.combo)[0:8] == str(row.datapoint)

        # The following function should match every row in the frame and
        # therefore leave it empty. It verifies we captured the correct data
        # in the new frame from the old one.
        twos.drop_rows(lambda row:
                       row.combo[0:8] == row.datapoint and
                       str(row.combo)[-1] == "2")
        self.assertEqual(twos.row_count, 0)

        ia.drop_frames([self.frame_pred, sevens, twos])

    def test_copy_projection(self):
        """
        Create various projections to a new table.
        """

        orig_take = self.frame.take(self.frame.row_count)

        # print "Create new frame and change the names of the columns"
        frame_2col = self.frame.copy(
            {'col_A': 'Column_A', 'Double': 'Float_64'})
        self.assertEqual(frame_2col.row_count, self.frame.row_count)

        # print "Create a new frame using 3 columns from the original"
        frame_3col = ia.Frame(self.frame[['col_B', 'Double', 'Text']])
        self.assertEqual(frame_3col.row_count, self.frame.row_count)

        # print "Create new frame using a single column"
        frame_1col = ia.Frame(self.frame[['Text']])
        self.assertEqual(frame_1col.row_count, self.frame.row_count)

        # print "Now verify master is unaltered:"
        end_take = self.frame.take(self.frame.row_count)
        self.assertEqual(orig_take, end_take)

        errors = self.frame.get_error_frame()
        if errors:
            print "Some data failed to import."
            print errors
            print errors.inspect(errors.row_count)
            ia.drop_frames(errors)

        ia.drop_frames([self.frame, frame_2col, frame_3col, frame_1col])


if __name__ == "__main__":
    unittest.main()
