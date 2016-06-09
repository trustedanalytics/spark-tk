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
   Usage:  python2.7 frame_sort_test.py
   Test interface functionality of
        frame.sort
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.09.01"

# Functionality tested:
#   frame.sort(columns, ascending=True)
#     single column, up implicit
#     single column, up explicit
#     single column, down explicit
#     multiple columns, up
#     multiple columns, down
#     multiple columns, mixed
#   error tests
#     bad column name
#     empty frame
#     dropped old frame
#     no column name
#
#   frame.sorted_k(k, column_names_and_ascending, reduce_tree_depth=None)
#     single column, up implicit
#     single column, up explicit
#     single column, down explicit
#     multiple columns, up
#     multiple columns, down
#     multiple columns, mixed
#   error tests
#     bad values for k and reduce_tree_depth
#     bad column name
#     empty frame
#     no column name


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import requests
import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


src_col = 0
wt_col = 3
e_col = 4
take_size = 20


class FrameSortTest(atk_test.ATKTestCase):
    """Test fixture the Frame sort function"""
    frame = None

    def setUp(self):
        super(FrameSortTest, self).setUp()

        # create standard, defunct, and empty frames"
        self.dataset = "movie.csv"
        self.schema = [("src", ia.int32),
                       ("dir", str),
                       ("dest", ia.int32),
                       ("weight", ia.int32),
                       ("e_type", str)]

        self.frame = frame_utils.build_frame(
            self.dataset, self.schema, skip_header_lines=1)

    def test_frame_sort_001(self):
        """
        Test single-column sorting
        """
        print "Sort ascending & descending, one column"

        print "Original file"
        unsorted = self.frame.take(take_size)
        print self.frame.row_count, "rows\n", unsorted
        print self.frame.inspect(10)
        print type(self.frame.inspect(0))

        print "Sort up, default"
        self.frame.sort("weight")
        up_take = self.frame.take(take_size)
        # print self.frame.inspect(10)

        for i in range(0, len(up_take)-1):
            self.assertLessEqual(
                up_take[i][wt_col], up_take[i+1][wt_col],
                "Sort out of order at row %d; %s <= %s" %
                (i, up_take[i][wt_col], up_take[i+1][wt_col]))

        print "Sort down"
        self.frame.sort("weight", False)
        down_take = self.frame.take(take_size)
        # print self.frame.inspect(10)

        for i in range(0, len(down_take) - 1):
            self.assertGreaterEqual(
                down_take[i][wt_col], down_take[i + 1][wt_col],
                "Sort out of order at row %d; %s >= %s" %
                (i, down_take[i][wt_col], down_take[i + 1][wt_col]))

        print "Sort up, explicit"
        self.frame.sort("weight", True)
        up_take_expl = self.frame.take(take_size)
        print up_take_expl

        for i in range(0, len(up_take_expl) - 1):
            self.assertLessEqual(
                up_take_expl[i][wt_col], up_take_expl[i+1][wt_col],
                "Sort out of order at row %d; %s <= %s" %
                (i, up_take_expl[i][wt_col], up_take_expl[i+1][wt_col]))

        self.assertSequenceEqual(up_take, up_take_expl)

    def test_frame_sort_002(self):
        """
        Test multiple-column sorting
        """
        print "Sort ascending & descending, multiple columns"

        print "Original file"
        unsorted = self.frame.take(take_size)
        print self.frame.row_count, "rows\n", unsorted

        print "Sort up, 2 cols"
        self.frame.sort(["weight", "e_type"])
        up_take = self.frame.take(take_size)
        # print self.frame.inspect(take_size)

        # This detailed check will return the first failure, with
        # appropriate detail.
        for i in range(0, len(up_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if up_take[i][wt_col] == up_take[i + 1][wt_col]:
                self.assertLessEqual(
                    up_take[i][e_col], up_take[i + 1][e_col],
                    "Edge_type out of order at row %d: %s vs %s" %
                    (i, up_take[i][e_col], up_take[i + 1][e_col]))
            else:
                self.assertLess(
                    up_take[i][wt_col], up_take[i + 1][wt_col],
                    "Weight out of order at row %d: %d vs %d" %
                    (i, up_take[i][wt_col], up_take[i + 1][wt_col]))

        print "Sort down, 2 cols"
        self.frame.sort([("weight", False), ("e_type", False)])
        down_take = self.frame.take(take_size)
        # print self.frame.inspect(take_size)

        # This detailed check will return the first failure, with appropriate
        # detail.
        for i in range(0, len(down_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if down_take[i][wt_col] == down_take[i + 1][wt_col]:
                self.assertGreaterEqual(
                    down_take[i][e_col], down_take[i + 1][e_col],
                    "Edge_type out of order at row %d: %s vs %s" %
                    (i, down_take[i][e_col], down_take[i + 1][e_col]))
            else:
                self.assertGreater(
                    down_take[i][wt_col], down_take[i + 1][wt_col],
                    "Weight out of order at row %d: %d vs %d" %
                    (i, down_take[i][wt_col], down_take[i + 1][wt_col]))

        print "Sort mixed, 3 cols"
        self.frame.sort([("weight", False), ("e_type", True), ("src", True)])
        self.frame.take(take_size)
        print self.frame.inspect(take_size)

    def test_frame_sort_error(self):
        """
        Test error cases:
            Sort empty frame
            Sort bad column names
            Sort with no columns specified
        """
        print "normal frame -- dummy column name"

        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sort, 'no-such-column')

        print "no columns specified"
        self.assertRaises(TypeError, self.frame.sort)

    def test_frame_sortedk_col_single(self):
        """
        Test single-column sorting
        """
        print "Sort ascending & descending, one column"

        print "Original file"
        unsorted = self.frame.take(take_size)
        print self.frame.row_count, "rows\n", unsorted
        print self.frame.inspect(10)

        print "Sort down"
        topk_frame = self.frame.sorted_k(5, [("weight", False)])
        down_take = topk_frame.take(take_size)
        # print self.frame.inspect(10)

        for i in range(0, len(down_take) - 1):
            self.assertGreaterEqual(
                down_take[i][wt_col], down_take[i + 1][wt_col],
                "Sort out of order at row %d; %s >= %s" %
                (i, down_take[i][wt_col], down_take[i + 1][wt_col]))

        print "Sort up"
        topk_frame = self.frame.sorted_k(5, [("weight", True)])
        up_take_expl = topk_frame.take(take_size)
        print up_take_expl

        for i in range(0, len(up_take_expl) - 1):
            self.assertLessEqual(
                up_take_expl[i][wt_col], up_take_expl[i+1][wt_col],
                "Sort out of order at row %d; %s <= %s" %
                (i, up_take_expl[i][wt_col], up_take_expl[i+1][wt_col]))

    def test_frame_sortedk_col_multiple(self):
        """
        Test multiple-column sorting
        """
        print "Sort ascending & descending, multiple columns"

        print "Original file"
        # unsorted = self.frame.take(take_size)
        print self.frame.row_count, "rows\n", self.frame.inspect()

        print "Sort up, 2 cols"
        topk_frame = self.frame.sorted_k(
            5, [("weight", True), ("e_type", True)])
        up_take = topk_frame.take(take_size)
        print topk_frame.inspect()

        # This detailed check will return the first failure, with
        # appropriate detail.
        for i in range(0, len(up_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if up_take[i][wt_col] == up_take[i + 1][wt_col]:
                self.assertLessEqual(
                    up_take[i][e_col], up_take[i + 1][e_col],
                    "Edge_type out of order at row %d: %s vs %s" %
                    (i, up_take[i][e_col], up_take[i + 1][e_col]))
            else:
                self.assertLessEqual(
                    up_take[i][wt_col], up_take[i + 1][wt_col],
                    "Weight out of order at row %d: %d vs %d" %
                    (i, up_take[i][wt_col], up_take[i + 1][wt_col]))

        print "Sort down, 2 cols"
        topk_frame = self.frame.sorted_k(
            5, [("weight", False), ("e_type", False)])
        down_take = topk_frame.take(take_size)
        print topk_frame.inspect(take_size)

        # This detailed check will return the first failure, with appropriate
        # detail.
        for i in range(0, len(down_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if down_take[i][wt_col] == down_take[i + 1][wt_col]:
                self.assertGreaterEqual(
                    down_take[i][e_col], down_take[i + 1][e_col],
                    "Edge_type out of order at row %d: %s vs %s" %
                    (i, down_take[i][e_col], down_take[i + 1][e_col]))
            else:
                self.assertGreaterEqual(
                    down_take[i][wt_col], down_take[i + 1][wt_col],
                    "Weight out of order at row %d: %d vs %d" %
                    (i, down_take[i][wt_col], down_take[i + 1][wt_col]))

        print "Sort mixed, 3 cols"
        topk_frame = self.frame.sorted_k(
            5, [("src", False), ("e_type", True), ("weight", True)])
        mixed_take = topk_frame.take(take_size)
        print topk_frame.inspect()

        # This detailed check will return the first failure, with appropriate
        # detail.
        for i in range(0, len(mixed_take) - 1):
            # If 1st sort key is equal, compare the 2nd
            if mixed_take[i][src_col] == mixed_take[i + 1][src_col]:
                # If 2nd sort key is also equal, compare the 3rd
                if mixed_take[i][wt_col] == mixed_take[i + 1][wt_col]:
                    self.assertGreaterEqual(
                        mixed_take[i][e_col], mixed_take[i + 1][e_col],
                        "Edge_type out of order at row %d: %s vs %s" %
                        (i, mixed_take[i][e_col], mixed_take[i + 1][e_col]))
                else:
                    self.assertGreaterEqual(
                        mixed_take[i][wt_col], mixed_take[i + 1][wt_col],
                        "Weight out of order at row %d: %d vs %d" %
                        (i, mixed_take[i][wt_col], mixed_take[i + 1][wt_col]))
            else:
                self.assertLessEqual(
                    mixed_take[i][src_col], mixed_take[i + 1][src_col],
                    "Weight out of order at row %d: %d vs %d" %
                    (i, mixed_take[i][src_col], mixed_take[i + 1][src_col]))

    def test_frame_sortedk_error(self):
        """
        Test error cases:
        """
        # k value negative, 0, more than row_count, float
        # Sort bad column names
        # Sort with no columns specified

        # Bad values for k
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, "5", [("weight", False)])

        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, -1, [("weight", False)])

        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, 0, [("weight", False)])

        # DPAT-627
        # self.assertRaises((ia.rest.command.CommandServerError,
        #                    requests.exceptions.HTTPError),
        #                   self.frame.sorted_k, 1E6, [("weight", False)])
        #
        # self.assertRaises((ia.rest.command.CommandServerError,
        #                    requests.exceptions.HTTPError),
        #                   self.frame.sorted_k, 3.14159, [("weight", False)])

        # Bad values for reduce_tree_depth
        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, 5, [("weight", False)],
                          reduce_tree_depth="5")

        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, 5, [("weight", False)],
                          reduce_tree_depth=-1)

        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, 5, [("weight", False)],
                          reduce_tree_depth=0)

        # DPAT-627
        # self.assertRaises((ia.rest.command.CommandServerError,
        #                    requests.exceptions.HTTPError),
        #                   self.frame.sorted_k, 5, [("weight", False)],
        #                   reduce_tree_depth=1E6)
        #
        # self.assertRaises((ia.rest.command.CommandServerError,
        #                    requests.exceptions.HTTPError),
        #                   self.frame.sorted_k, 5, [("weight", False)],
        #                   reduce_tree_depth=3.14159)

        print "normal frame -- dummy column name"

        self.assertRaises((ia.rest.command.CommandServerError,
                           requests.exceptions.HTTPError),
                          self.frame.sorted_k, 5, [('no-such-column', True)])

        print "no columns specified"
        self.assertRaises(TypeError, self.frame.sort)


if __name__ == "__main__":
    unittest.main()
