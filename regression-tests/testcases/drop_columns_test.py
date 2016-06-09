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
# import iatest
# iatest.init()
# import sys
"""
    Tests the drop columns method on frames
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class TestDropColumns(atk_test.ATKTestCase):

    def setUp(self):
        """Build ther requisite frames."""
        super(TestDropColumns, self).setUp()
        dataset = "DropColData.csv"
        # Define the schema
        schema = [("col_A", ia.int32),
                  ("col_B", ia.int32),
                  ("col_C", ia.int32)]
        # import the frames from the added files
        self.frame = frame_utils.build_frame(
            dataset, schema, skip_header_lines=1, prefix=self.prefix)

        print "Created frame:", self.frame.name

    def test_drop_1_column(self):
        """Drop first column."""
        frame = self.frame.copy()
        frame.drop_columns('col_A')
        self.assertEqual(frame.column_names, ['col_B', 'col_C'])

    def test_drop_2_columns(self):
        """Drop middle column."""
        frame = self.frame.copy()
        frame.drop_columns(['col_A', 'col_C'])
        self.assertEqual(frame.column_names, ['col_B'])


if __name__ == '__main__':
    unittest.main()
