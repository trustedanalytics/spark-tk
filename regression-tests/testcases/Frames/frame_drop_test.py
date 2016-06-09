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
   Usage:  python2.7 frame_drop_test.py
   Test interface functionality of
        ia.drop_frames(frame)
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "29.10.2014.001"


# Functionality tested:
#   ia.drop_frames(frame)
#     valid frame without name
#     valid frame with name
#     drop by name
#     drop by file handle
#     drop all frames (don't do this -- may interfere with parallel tests)
#     multiple frames
#     copy frame; drop original; copy should be alive
#     copy frame; drop copy; original should be alive
#     empty frame
#     dropped frame (error)
#     None frame (error)
#    non-existent frame name (error)

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class FrameDropTest(atk_test.ATKTestCase):

    def setUp(self):
        """create standard frames for general use"""
        super(FrameDropTest, self).setUp()

        self.dataset = "typesTest3.csv"
        self.schema = [("col_A", ia.int32),
                       ("col_B", ia.int64),
                       ("col_C", ia.float32),
                       ("Double", ia.float64),
                       ("Text", str)]

        self.frame = frame_utils.build_frame(self.dataset, self.schema)
        self.name = self.frame.name

    def test_no_drop(self):
        """Tests that dropping no frame does nothing."""
        # print "Drop empty list"
        ia.drop_frames([])
        # Check that the test's only created frame is still in the list.
        self.assertIn(self.name, ia.get_frame_names())

    def test_drop_empty_frame(self):
        """Tests that building and dropping an empty frame exists and
           then doesn't.
        """
        # print "Drop empty frame"
        hardened_name = common_utils.get_a_name("empty_frame")
        empty_frame = ia.Frame(name=hardened_name)
        self.assertIn(hardened_name, ia.get_frame_names())
        ia.drop_frames(empty_frame)
        self.assertNotIn(hardened_name, ia.get_frame_names())

    def test_drop_frame_handle(self):
        """Test dropping by the frame handle."""
        name = self.frame.name
        ia.drop_frames(self.frame)
        self.assertNotIn(name, ia.get_frame_names())

    def test_drop_specified_name(self):
        """Tests dropping a frame with a specified name"""
        # print "Vanilla Frame, name specified"
        ia.drop_frames(self.name)
        self.assertNotIn(self.name, ia.get_frame_names())

    def test_drop_multiple_frames(self):
        """Test dropping multiple frames"""
        # print "Drop multiple frames"
        # print "Drop by name"
        # Add three frames and verify that they exist.

        # Frames are built here with unique names to be dropped
        name_list = ["1st_Little_Pyg", "2nd_Little_Pyg", "3rd_Little_Pyg"]
        real_names = []
        for pyg_name in name_list:
            name = frame_utils.build_frame(
                self.dataset, self.schema, prefix=pyg_name).name
            real_names.append(name)
        master_list = ia.get_frame_names()
        for name in real_names:
            self.assertIn(name, master_list,
                          "'Little_Pyg' frame not added for drop test")

        # Drop three frames at once and verify that they're gone.
        ia.drop_frames(real_names)
        master_list = ia.get_frame_names()
        for name in real_names:
            self.assertNotIn(name, master_list,
                             "'Little_Pyg' frame not dropped")

    def test_drop_copy_original(self):
        """Test dropping a copy does not affect the original"""
        # TRIB-4017, 4019
        # print "Dropping copy does not affect original"
        copy_name = common_utils.get_a_name("Copy")
        copy_frame = ia.Frame(self.frame, copy_name)
        ia.drop_frames(copy_frame)
        master_list = ia.get_frame_names()
        # print master_list
        self.assertNotIn(copy_name, master_list, "Copy not properly dropped")
        self.assertIn(self.name, ia.get_frame_names(),
                      "Original dropped unjustly")

    def test_drop_original_copy(self):
        """Dropping an original frame should not affect the copies."""
        copy_name = common_utils.get_a_name("Copy")
        frame_name = self.frame.name
        copy_frame = ia.Frame(self.frame, copy_name)
        self.assertEqual(
            copy_frame.name, copy_name,
            ("Name of copy not as specified"
             "https://jira01.devtools.intel.com/browse/TRIB-4218"))
        ia.drop_frames(self.frame)
        master_list = ia.get_frame_names()
        # print master_list
        self.assertNotIn(frame_name, master_list,
                         "Original not properly dropped")
        self.assertIn(copy_name, master_list,
                      ("Copy dropped unjustly"
                       "https://jira01.devtools.intel.com/browse/TRIB-4171"))
        ia.drop_frames(copy_frame)

    def test_drop_null(self):
        """Dropping null should error."""
        # print "Drop a null"
        self.assertRaises(TypeError, ia.drop_frames, None)

    def test_drop_non_existent_frame(self):
        """Dropping a non-existent frame should return no dropped items."""

        name_bad = "No_such_frame"
        self.assertEquals(0, ia.drop_frames(name_bad))


if __name__ == "__main__":
    unittest.main()
