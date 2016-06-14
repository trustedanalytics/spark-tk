##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
'''
   Usage:  python2.7 frame_stats_test.py
   Test that Frame statistic methods are properly
   exposed by Seamless Graph class
'''

__author__ = "WDW"
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "2015.01.02"


"""
Functionality tested:
  column_mode

Replaces tests
  Manual
    mode_stats2
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class FrameStatsTest(atk_test.ATKTestCase):

    def setUp(self):
        super(FrameStatsTest, self).setUp()

        self.data_stat = "mode_stats.tsv"
        self.schema_stat = [("weight", ia.float64), ("item", str)]

    def test_column_mode(self):
        """
        Validate column mode.
        """

        stat_frame = frame_utils.build_frame(
            self.data_stat, self.schema_stat, delimit='\t')
        stats = stat_frame.column_mode("item", "weight",
                                       max_modes_returned=50)
        expected_mode = {'Poliwag', 'Pumpkaboo', 'Scrafty',
                         'Psyduck', 'Alakazam'}

        print stats
        for k, v in stats.iteritems():
            print "%s \t %s" % (k, v)
        self.assertEqual(stats["mode_count"], 5)
        self.assertEqual(stats["total_weight"], 1749)
        self.assertEqual(set(stats["modes"]), expected_mode)


if __name__ == "__main__":
    unittest.main()
