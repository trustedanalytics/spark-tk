##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014,2015 Intel Corporation All Rights Reserved.
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
# #############################################################################
"""
usage:
python2.7 binning.py

Tests the binning functionality. This function adds an extra column
containing the bin number for each branch, based on the column. Binning is
either equal depth or equal width. These values are compared with known
good values.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class BinningHarness(atk_test.ATKTestCase):

    def setUp(self):
        """
        The following values were found out manually by running the binning
        outside of the system. These are used to compare the results with the
        one found after execution. These values are tied to the datasets being
        used in this harness, if there is any change in the datasets then these
        values needs to be evaluated and modified.
        """
        super(BinningHarness, self).setUp()
        self.schema = [("user_id", ia.int32),
                       ("vertex_type", str),
                       ("movie_id", ia.int32),
                       ("rating", ia.int32),
                       ("splits", str)]

        self.netf_5_ratings = "netf_5_ratings.csv"
        self.netf_all_3 = "netf_all_3.csv"
        self.netf_1_2_5 = "netf_1_2_5.csv"

        self.equalwidth_n_equaldepth_5bins_5ratings = [[0, 3970],
                                                       [1, 3744],
                                                       [2, 3994],
                                                       [3, 3950],
                                                       [4, 3944]]

        self.equalwidth_2bins_5ratings = [[0, 7714], [1, 11888]]

        self.equaldepth_2bins_5ratings = [[0, 11708], [1, 7894]]

        self.equalwidth_10bins_5ratings = [[0, 3970],
                                           [2, 3744],
                                           [5, 3994],
                                           [7, 3950],
                                           [9, 3944]]

        self.equaldepth_10bins_5ratings = [[0, 3970],
                                           [1, 3744],
                                           [2, 3994],
                                           [3, 3950],
                                           [4, 3944]]

        self.equalwidth_n_equaldepth_5bins_1rating = [[0, 19602]]

        self.equaldepth_1_2_5_ratings_2bins = [[0, 7714], [1, 11888]]

        # Movie user data with original ratings
        # Big data frame from data with 5 ratings
        self.frame_5ratings = frame_utils.build_frame(
            self.netf_5_ratings, self.schema, self.prefix)
        # Movie user data with some missing ratings
        # Big data frame from data with only 1 ratings
        self.frame_1ratings = frame_utils.build_frame(
            self.netf_all_3, self.schema, self.prefix)

        # Movie user data with some missing ratings
        # Big data frame from data with ratings 1, 2 and 5 only.
        self.frame_equaldepth_2bins_1_2_5_ratings = frame_utils.build_frame(
            self.netf_1_2_5, self.schema, self.prefix)

    def test_equalwidth_5bins_5ratings(self):
        """Equal width binning on 5 ratings into 5 bin
           Binning of frame
        """
        self.frame_5ratings.bin_column_equal_width("rating", 5, "binned0")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_5ratings.group_by("binned0", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equalwidth_n_equaldepth_5bins_5ratings)

    def test_equalwidth_2bins_5ratings(self):
        """Equal width  binning on 5 ratings into 2 bins
           binning of frame
        """
        self.frame_5ratings.bin_column_equal_width("rating", 2, "binned1")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_5ratings.group_by("binned1", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equalwidth_2bins_5ratings)

    def test_equalwidth_10bins_5ratings(self):
        """Equal Width binning on 5 ratings and 10 bins
           binning of frame
        """
        self.frame_5ratings.bin_column_equal_width("rating", 10, "binned2")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_5ratings.group_by("binned2", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equalwidth_10bins_5ratings)

    def test_equaldepth_5bins_5ratings(self):
        """Equal depth binning on 5 ratings into 5 bins
           binning of frame
        """
        self.frame_5ratings.bin_column_equal_depth("rating", 5, "binned3")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_5ratings.group_by("binned3", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equalwidth_n_equaldepth_5bins_5ratings)

    def test_equaldepth_2bins_5ratings(self):
        """Equal depth binning on 5 ratings into 2 bins
           binning of frame
        """
        self.frame_5ratings.bin_column_equal_depth("rating", 2, "binned4")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_5ratings.group_by("binned4", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equaldepth_2bins_5ratings)

    def test_equaldepth_10bins_5ratings(self):
        """Equal depth binning on 5 ratings into 10 bins
           binning of frame.
        """
        self.frame_5ratings.bin_column_equal_depth("rating", 10, "binned5")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_5ratings.group_by("binned5", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equaldepth_10bins_5ratings)

    def test_equalwidth_5bins_1rating(self):
        """Equal width binning on 1 rating and 5 bins
           binning of frame.
        """
        self.frame_1ratings.bin_column_equal_width("rating", 5, "binned6")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_1ratings.group_by("binned6", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equalwidth_n_equaldepth_5bins_1rating)

    def test_equaldepth_5bins_1rating(self):
        """Equal depth binning on 1 rating and 5 bins
           binning of frame>
        """
        self.frame_1ratings.bin_column_equal_depth("rating", 5, "binned7")
        # groupby of frame on the count of binned column.
        groupby_frame = self.frame_1ratings.group_by("binned7", ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equalwidth_n_equaldepth_5bins_1rating)

    def test_equaldepth_2bins_1_2_5_rating(self):
        """Equal depth binning on ratings 1, 2 and 5 into
           2 bins # binning of frame.
        """
        self.frame_equaldepth_2bins_1_2_5_ratings.bin_column_equal_depth(
            "rating", 2, "binned8")
        groupby_frame = \
            self.frame_equaldepth_2bins_1_2_5_ratings.group_by("binned8",
                                                               ia.agg.count)
        groupby_result = groupby_frame.take(10)
        self.assertEqual(sorted(groupby_result),
                         self.equaldepth_1_2_5_ratings_2bins)


if __name__ == '__main__':
    unittest.main()
