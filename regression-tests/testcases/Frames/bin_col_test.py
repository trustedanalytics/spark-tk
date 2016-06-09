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
   Usage:  python2.7 bin_col_test.py
   Test interface functionality of
        frame.bin_columns()
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.02.09"

"""
Functionality tested:
  frame.bin_columns(n=rows, offset=rows, columns=[names])
  test name collision on auto-named columns
    Quantile features
    cutoffs
      list / tuple
      one cutoff
      multiple cutoffs
      descending monotonic
      error: empty list
      error: None
      error: non-monotonic values
    include_lowest
      True / False / default
      data set must include the cutoff
    strict_binning
      True / False / default
      Data values beyond the cutoffs
    bin_column_name
      Default / assigned
      error: Use existing name
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
from itertools import count

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class BinColTest(atk_test.ATKTestCase):

    def setUp(self):
        super(BinColTest, self).setUp()
        # create standard and defunct frames for general use"

        self.dataset = "netflix-1200.csv"
        self.schema = [("src", ia.int32),
                       ("direction", str),
                       ("expect_1", ia.int32),
                       ("expect_2", ia.int32),
                       ("tr_te_va", str)]
        self.frame = frame_utils.build_frame(
            self.dataset, self.schema, skip_header_lines=1)

        self.file_rows = 1200 - 1   # first row is column names
        self.cutoff_list = [-10014, -4408, -1101, -389]
        self.cutoff_desc = self.cutoff_list[::-1]
        self.cutoff_tuple = tuple(self.cutoff_list)
        self.cutoff_monad = [-131]
        self.cutoff_diad = [-10014, -131]

    def test_bin_column_name_collision(self):
        """
        Validate that default column naming avoids name collision.
        The schema covers several names that would normally be generated.
        """
        # print "Test name collision"
        rename_dir = {"direction": "src_binned_1",
                      "expect_1": "src_binned_0",
                      "expect_2": "src_binned",
                      "tr_te_va": "src_binned_0_1_2"}
        self.frame.rename_columns(rename_dir)

        # Call bin_col multiple times to force collisions with column names.
        self.frame.bin_column_equal_depth("src", 10)
        print self.frame.inspect(50)
        self.frame.bin_column_equal_depth("src", 10)
        print self.frame.inspect(50)
        self.frame.bin_column_equal_depth("src", 10)
        print self.frame.inspect(50)
        self.assertIn("src_binned_0_1", self.frame.column_names)
        self.assertIn("src_binned_0_1_2_3", self.frame.column_names)
        self.assertIn("src_binned_0_1_2_3_4", self.frame.column_names)

        self.frame.drop_columns("src_binned_0")
        self.assertNotIn("src_binned_0", self.frame.column_names)
        self.frame.bin_column_equal_depth("src", 10)
        print self.frame.inspect(50)
        self.assertIn("src_binned_0", self.frame.column_names)

    def test_bin_column_cutoff_multi(self):
        # multiple cutoffs
        # cutoff tuple
        # ascending
        # include_lowest = default (True)
        # strict_binning = default (False)
        # bin_column_name = default
        self.frame.drop_columns(["expect_1", "expect_2"])

        print self.cutoff_list, self.cutoff_tuple
        self.frame.bin_column("src", self.cutoff_list)
        self.frame.sort("src")
        print self.frame.inspect(self.frame.row_count)
        frame_take = self.frame.take(self.frame.row_count)

        # Return a list of row numbers matching each cutoff value
        #   "i" is a sequence number for a "row" from the frame.
        #   "zip" is a list-join function.
        #   "cut" is an element of the cutoff_list.
        cutoff_list = [[i for i, row in zip(count(), frame_take)
                        if row[0] == cut]
                       for cut in self.cutoff_list]
        cutoff_count = len(cutoff_list)
        for cutoff_num in range(0, cutoff_count):
            # Verify that the last element in the row is the expected bin
            # The last cutoff goes into the previous bin (min limit below)
            pos_list = cutoff_list[cutoff_num]
            if len(pos_list) > 0:
                cut_row = frame_take[pos_list[0]]
                print cutoff_num, cut_row
                self.assertEqual(min(cutoff_num, cutoff_count-2), cut_row[-1])

    def test_bin_column_tuple_params(self):
        # multiple cutoffs
        # cutoff list
        # ascending
        # include_lowest = True
        # strict_binning = False
        # bin_column_name = given
        self.frame.drop_columns(["expect_1", "expect_2"])

        print self.cutoff_list, self.cutoff_tuple
        self.frame.bin_column("src",
                              self.cutoff_tuple,
                              include_lowest=True,
                              strict_binning=False,
                              bin_column_name="multi_bin")
        self.frame.sort("src")
        print self.frame.inspect(self.frame.row_count)
        frame_take = self.frame.take(self.frame.row_count)

        # Find the indices of each cutoff value
        cutoff_list = [[i for i, row in zip(count(), frame_take)
                        if row[0] == cut]
                       for cut in self.cutoff_tuple]
        cutoff_count = len(cutoff_list)
        for cutoff_num in range(0, cutoff_count):
            # Verify that the last element in the row is the expected bin
            # The last cutoff goes into the previous bin (min limit below)
            pos_list = cutoff_list[cutoff_num]
            if len(pos_list) > 0:
                cut_row = frame_take[pos_list[0]]
                print cutoff_num, cut_row
                self.assertEqual(min(cutoff_num, cutoff_count-2), cut_row[-1])

    def test_bin_column_one_bin(self):
        # single bin
        # cutoff list
        # ascending NA
        # include_lowest = False
        # strict_binning = True
        # bin_column_name = assigned
        self.frame.drop_columns(["expect_1", "expect_2"])

        self.frame.bin_column("src",
                              self.cutoff_diad,
                              include_lowest=False,
                              strict_binning=True,
                              bin_column_name="single_bin")
        self.frame.sort("src")
        # print self.frame.inspect(self.frame.row_count)
        frame_take = self.frame.take(self.frame.row_count)

        # Find the indices of each cutoff value
        cutoff_list = [[i for i, row in zip(count(), frame_take)
                        if row[0] == cut]
                       for cut in self.cutoff_diad]
        cutoff_count = len(cutoff_list)
        for cutoff_num in range(0, cutoff_count):
            # Verify that the last element in the row is the expected bin
            # The last cutoff goes into the previous bin (min limit below)
            pos_list = cutoff_list[cutoff_num]
            if len(pos_list) > 0:
                cut_row = frame_take[pos_list[0]]
                # print "CUTOFF:", cutoff_num, cut_row
                self.assertEqual(
                    max(0, cutoff_num-1), cut_row[-1],
                    "Endpoint in wrong bin "
                    "https://jira01.devtools.intel.com/browse/TRIB-4480")

    def test_bin_column_cutoff_monad(self):
        # single cutoff (no legal bin)
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "src",
                          self.cutoff_monad)

        print self.frame.inspect(50)

    def test_bin_column_bad_col(self):
        # Bad column name should be gently rejected
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "non_existent_name",
                          self.cutoff_list,
                          bin_column_name="tr_te_va_bin")

    def test_bin_column_cutoff_string(self):
        # Array of characters should get an exception
        # self.assertRaises((ValueError, TypeError),
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "tr_te_va",
                          "su",
                          bin_column_name="tr_te_va_bin")

    def test_bin_column_cutoff_string_list(self):
        # List of strings should get an exception
        # self.assertRaises((ValueError, TypeError),
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "tr_te_va",
                          ["te", "tr"],
                          bin_column_name="tr_te_va_bin")

    def test_bin_column_cutoff_none(self):
        # None cutoff list should be rejected
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "src",
                          None)

    def test_bin_column_cutoff_empty(self):
        # Empty cutoff list should be rejected
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "src",
                          [])

    def test_bin_column_cutoff_mixed(self):
        """ Error case: cutoffs are not monotonic """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "src",
                          self.cutoff_list.append(-5511),
                          bin_column_name="no_bin")

    def test_bin_column_cutoff_desc(self):
        """ API doc allows monotonic in either direction """
        print "cutoff_desc=", self.cutoff_desc
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "src",
                          self.cutoff_desc)

    def test_bin_column_name_dup(self):
        """ Duplicate column name should fault """
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.frame.bin_column,
                          "src",
                          self.cutoff_desc,
                          bin_column_name='src')


if __name__ == "__main__":
    unittest.main()
