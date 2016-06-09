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
# This harness tests data split functionality on the frames. Data split adds
# a new column to the frame, this column contains the split tag for each
# row. A sample split could be 'Training', 'Test' and 'Validation'. This
# harness could take any data with the given schema and would test it for
# the split results.
##############################################################################
"""
usage:
python2.7 assign_sample_test.py

Tests data split functionality, which adds a column that tags each row. Tests
are generic across schema and data set.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class dataSplitter(atk_test.ATKTestCase):

    def setUp(self):
        """Build frames to be tested."""
        super(dataSplitter, self).setUp()

        self.first_sample = {'one': 0.6, 'two': 0.3, 'three': 0.1}
        self.second_sample = {'TR': 0.5, 'TE': 0.3, 'VA': 0.2}
        self.third_sample = {'Sample_0': 0.6,
                             'Sample_1': 0.2,
                             'Sample_2': 0.1,
                             'Sample_3': 0.1}

        self.table_name = "Original_frame"
        self.schema = [("color1", str), ("predicted", str)]

        self.frame = frame_utils.build_frame(
            "model_color.csv", self.schema, self.prefix)

    def _test_frame_assign(self, column_name, sample):
        """Tests the assign method on the given column and sample."""

        groupby_rows = self.frame.group_by(column_name,
                                           ia.agg.count).inspect().rows
        groupby_dict = {ele[0]: ele[1] for ele in groupby_rows}
        total_sum = sum(groupby_dict.values())
        groupby_percent_dict = {key: float(res) / float(total_sum)
                                for (key, res) in groupby_dict.items()}
        self.assertEqual(sorted(groupby_percent_dict.keys()),
                         sorted(sample.keys()))
        print groupby_percent_dict
        for key in groupby_percent_dict.keys():
            self.assertAlmostEqual(sample[key],
                                   groupby_percent_dict[key],
                                   delta=0.01)

    def test_label_column(self):
        """Test splitting on the label column"""
        self.frame.assign_sample([0.6, 0.3, 0.1],
                                 ['one', 'two', 'three'],
                                 'label_column', 2)
        self._test_frame_assign('label_column', self.first_sample)

    def test_sample_bin(self):
        """Test splitting on the sample_bin column"""
        self.frame.assign_sample([0.5, 0.3, 0.2])
        self._test_frame_assign("sample_bin", self.second_sample)

    def test_random_seed(self):
        """
        Check that the default seed is 0;
        check that another seed give different results.
        """
        self.frame.assign_sample([0.6, 0.2, 0.1, 0.1],
                                 output_column="default")
        self.frame.assign_sample([0.6, 0.2, 0.1, 0.1],
                                 random_seed=0, output_column="seed_0")
        self.frame.assign_sample([0.6, 0.2, 0.1, 0.1],
                                 random_seed=5, output_column="seed_5")

        # Check expected results
        self._test_frame_assign("default", self.third_sample)

        frame_take = self.frame.take(self.frame.row_count)
        seed_d = [_[2] for _ in frame_take]
        seed_0 = [_[3] for _ in frame_take]
        seed_5 = [_[4] for _ in frame_take]
        print seed_d[:10], '\n', seed_0[:10], '\n', seed_5[:10]
        # seed=0 and default give the same results.
        self.assertEqual(seed_0, seed_d)
        # seed=0 and seed=5 give different assignments.
        self.assertFalse(reduce((lambda a, b: a and b),
                                [seed_0[i] == seed_5[i]
                                 for i in range(len(seed_0))]))

if __name__ == '__main__':
    unittest.main()
