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
    Usage:  python2.7 file_load_test.py
    Test load/import to frames.
"""
__author__ = 'Prune Wickart'
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "17.12.2014.001"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class FileLoadTest(atk_test.ATKTestCase):

    def setUp(self):
        """Identify datafile / schema pairs"""
        super(FileLoadTest, self).setUp()

        self.datafile_empty = "empty2.csv"
        self.schema_empty = [("col_A", ia.int32),
                             ("col_B", ia.int64),
                             ("col_C", str),
                             ("col_D", ia.float32),
                             ("col_E", ia.float64),
                             ("col_F", str)]

        self.datafile_white = "TextWithSpaces_Tabs.csv"
        self.schema_white = [("col_A", str),
                             ("col_B", str),
                             ("col_C", str)]

    def test_import_empty(self):
        """Import an empty file."""

        frame = frame_utils.build_frame(self.datafile_empty, self.schema_empty)
        self.assertEqual(frame.row_count, 0)
        ia.drop_frames(frame)

        self.schema_empty.pop()

        # print "Build empty frame."
        frame = frame_utils.build_frame(self.datafile_empty, self.schema_empty)
        self.assertEqual(frame.row_count, 0)

        # print "Verify that copies of empty frame are empty."
        null2 = frame.copy()
        self.assertEqual(null2.row_count, 0)
        self.assertEqual(frame.schema, null2.schema)

        null3 = ia.Frame(frame)
        self.assertEqual(null3.row_count, 0)
        self.assertEqual(frame.schema, null3.schema)

        # Clean up
        ia.drop_frames([frame, null2, null3])

    def test_import_whitespace(self):
        """
        Import 3 text columns.
        Test various combinations of leading and trailing spaces and tabs,
          both with & without quotes around the string body.
        """
        file_size = 150

        # Create the frame and give it a name that's easy to spot
        white_frame = frame_utils.build_frame(
            self.datafile_white, self.schema_white)
        self.assertEqual(white_frame.row_count, file_size)

        # print "Append the three columns into a single-column frame."
        single = white_frame.copy("col_A")
        single.append(white_frame.copy({"col_B": "col_A"}))
        single.append(white_frame.copy({"col_C": "col_A"}))
        self.assertEqual(single.row_count, 3 * file_size)

        # print "Remove place-holder entries."
        single.drop_rows(lambda row: row["col_A"] == "Place-holder text.")
        self.assertEqual(single.row_count, file_size)

        # print "Drop the duplicates and see what's left."
        single.drop_duplicates()
        self.assertEqual(single.row_count, 49)

        # print "Check that two error rows failed to load."
        fails = white_frame.get_error_frame()
        self.assertIsNotNone(fails)
        self.assertEqual(fails.row_count, 2)

        # Clean up
        ia.drop_frames([white_frame, single, fails])


if __name__ == "__main__":
    unittest.main()
