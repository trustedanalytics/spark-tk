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
   Usage:  python2.7 frame_inspect_test.py
   Test interface functionality of
        frame.inspect()
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "05.12.2014.002"


# Functionality tested:
#   frame.inspect(n=rows, offset=rows, columns=[names])
#     n = 0
#     n = 1
#     n >> 10
#     n > file size (all rows)
#     offset = 0
#     offset = 1
#     offset large
#     offset > file size (no rows)
#     columns = None (default)
#     columns = None (explicit)
#     list all columns, in order
#     list all columns, out of order
#     list one column
#     list several columns
#     empty list
#   error tests
#     dropped frame
#     n = -1
#     offset < 0
#     offset non-integer
#
# Time: 999s
#     on a bare-metal 4-node cluster
#
# This test case replaces
#   harnesses
#     <none>
#   TUD
#     InspectZero

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import requests.exceptions
import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test

test_prefix = "inspect_frame"


class FrameInspectTest(atk_test.ATKTestCase):
    """The test fixture for frames.inspect()"""

    def setUp(self):
        # PyUnit standard routine
        super(FrameInspectTest, self).setUp()

        # create standard and defunct frames for general use"
        self.dataset = "netflix-1200.csv"
        self.schema = [("src", ia.int32),
                       ("vertex_type", str),
                       ("dest", ia.int32),
                       ("weight", ia.int32),
                       ("edge_type", str)]

        # import the frames from the added files
        self.frame = frame_utils.build_frame(
            self.dataset, self.schema, self.prefix, skip_header_lines=1)

        self.file_rows = 1200 - 1   # first row is column names

    @staticmethod
    def inspect_row_count(ins):
        """ Return the number of data rows in an "inspect" object.
        In normal cases, this is the value of "n" passed to
        frame.inspect(n = ??)
        :param ins: inspect object
        """
        n = len(ins.rows)
        return n

    def test_frame_inspect_basic(self):
        """Exercise normal operation of the frame.inspect() function.
          various offset values
          various "n" values (row counts)
        """
        #####
        #
        # print "--- exercise various offset values ---"
        #
        #####
        # print "self.frame:\n{0}"\
        #     .format(self.frame.inspect(self.frame.row_count))
        print "offset=0"
        row_count = 0
        inspection = self.frame.inspect(n=5, offset=row_count)
        self.assertEqual(self.inspect_row_count(inspection), 5)

        print "offset = just less than whole file"
        row_count = self.file_rows - 10
        inspection = self.frame.inspect(n=5, offset=row_count)
        self.assertEqual(self.inspect_row_count(inspection), 5)

        print "offset = size - <less than inspection>"
        row_count = self.file_rows - 3
        inspection = self.frame.inspect(n=10, offset=row_count)
        self.assertEqual(self.inspect_row_count(inspection), 3)

        #####
        #
        # print "--- exercise various row counts ---"
        #
        #####

        print "n=0; header only"
        row_count = 0
        inspection = self.frame.inspect(n=row_count)
        self.assertEqual(self.inspect_row_count(inspection), row_count)

        print "n=1; header & one line"
        row_count = 1
        inspection = self.frame.inspect(n=row_count)
        self.assertEqual(self.inspect_row_count(inspection), row_count)

        print "n=5; header & several lines"
        row_count = 5
        inspection = self.frame.inspect(n=row_count)
        self.assertEqual(self.inspect_row_count(inspection), row_count)

        print "n >> 10; header & several lines"
        row_count = 500
        inspection = self.frame.inspect(n=row_count)
        self.assertEqual(self.inspect_row_count(inspection), row_count)

        print "n = file_size; all lines"
        row_count = self.file_rows
        inspection = self.frame.inspect(n=row_count)
        self.assertEqual(self.inspect_row_count(inspection), row_count)

        print "n > file_size; all lines"
        row_count = self.file_rows * 10
        inspection = self.frame.inspect(n=row_count)
        self.assertEqual(self.inspect_row_count(inspection), self.file_rows)

    def test_frame_inspect_error(self):
        """inspect negative testing
          offset < 0
          n < 0     row count
          fractional rows
        """
        print "Inspect negative offset"
        self.assertRaises(RuntimeError, self.frame.inspect, n=5, offset=-1)

        print "Inspect negative rows"
        self.assertRaises(ValueError, self.frame.inspect, n=-1)

        print "Inspect non-integer rows"
        self.assertRaises(RuntimeError, self.frame.inspect, n=1.5)

    def test_take_inspect_error(self):
        """
        frame.take negative testing
          offset < 0
          n < 0
          fractional rows
        """
        print "take a negative slice"
        self.assertRaises(ValueError, self.frame.take, n=-1)

        print "take a negative offset"
        self.assertRaises(TypeError, self.frame.take, offset=-1)

        print "take non-integer rows"
        self.assertRaises(RuntimeError, self.frame.take, n=1.5)

    def test_take_inspect_error_091(self):
        """
        frame.take negative testing uncovered in 0.9.1
          non-existent column
          empty column list
        """
        # TRIB-4249 -- fixed
        print "take empty column list"
        # Demonstrate the error: list of empty lists returned
        # print self.frame.take(n=10, columns=[])
        self.assertRaises(
            (TypeError, ValueError, requests.exceptions.HTTPError),
            self.frame.take, n=10, columns=[])

        # TRIB-4247 -- fixed
        print "take non-existent column"
        # Demonstrate the error: one column returned
        # print self.frame.take(n=10, columns=["no_such_col", "weight"])
        self.assertRaises(
            (TypeError, ValueError, requests.exceptions.HTTPError),
            self.frame.take, n=10, columns=["no_such_col", "weight"])


if __name__ == "__main__":
    unittest.main()
