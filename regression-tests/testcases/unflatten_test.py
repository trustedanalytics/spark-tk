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
""" Tests unflatten functionality"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class Unflatten(atk_test.ATKTestCase):

    def setUp(self):
        """Create standard and defunct frames for general use"""
        super(Unflatten, self).setUp()
        # count of the unique user in the file is 5, readings for 1 day
        datafile_unflatten = "unflatten_data.csv"
        schema_unflatten = [("user", str),
                            ("day", str),
                            ("time", str),
                            ("reading", ia.int32)]

        self.frame = frame_utils.build_frame(
            datafile_unflatten, schema_unflatten, self.prefix)
        self.unique_count = 5

        # count of the unique user in the file is 1000, and
        # readings for 2 days
        datafile_unflatten_sparse = "unflatten_data_sparse.csv"
        schema_unflatten_sparse = [("user", ia.int32),
                                   ("day", str),
                                   ("time", str),
                                   ("reading", str)]

        self.frame_sparse = frame_utils.build_frame(
            datafile_unflatten_sparse, schema_unflatten_sparse, self.prefix)
        self.unique_count_sparse = 100

    def test_unflatten_basic(self):
        """ test for unflatten comma-separated rows """

        # call unflatten on user and day
        self.frame.unflatten_columns(['user', 'day'])
        unflat_copy = self.frame.download()
        for index, row in unflat_copy.iterrows():
            # both column should have same number of elements
            self.assertEqual(
                len(str(row['time']).split(',')),
                len(str(row['reading']).split(',')))
        # row count should be equal to number of unique users * days
        self.assertEqual(self.frame.row_count, 1 * self.unique_count)

        # call unflatten on user and day
        self.frame_sparse.unflatten_columns(['user', 'day'])
        unflat_sparse_copy = self.frame_sparse.download()
        for index, row in unflat_sparse_copy.iterrows():
            # both columns should have same number of elements
            self.assertEqual(
                len(str(row['time']).split(',')),
                len(str(row['reading']).split(',')))
        # row count should be equal to number of unique users * days
        self.assertEqual(
            self.frame_sparse.row_count, 2 * self.unique_count_sparse)


if __name__ == "__main__":
    unittest.main()
