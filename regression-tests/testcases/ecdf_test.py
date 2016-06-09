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
usage:
python2.7 ecdf_test.py

Tests the ECDF functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class ecdfTest(atk_test.ATKTestCase):

    def setUp(self):
        """
        Verify that the input and baselines exist before running the tests.
        """
        super(ecdfTest, self).setUp()

        # Movie user data with original ratings
        dataset_33 = "model_33_percent.csv"
        # Movie user data with some missing ratings
        dataset_netf = "netf_1_2_5.csv"

        schema_33 = [("user_id", ia.int32),
                     ("vertex_type", str),
                     ("movie_id", ia.int32),
                     ("rating", ia.int32),
                     ("splits", str),
                     ("predicted", ia.int32)]

        schema_netf = [("user_id", ia.int32),
                       ("vertex_type", str),
                       ("movie_id", ia.int32),
                       ("rating", ia.int32),
                       ("splits", str)]

        # Big data frame from data with 33% correct predicted ratings
        self.frame_33_percent = frame_utils.build_frame(
            dataset_33, schema_33, self.prefix)
        # Big data frame from data with only 1 ratings
        self.frame_netf = frame_utils.build_frame(
            dataset_netf, schema_netf, self.prefix)

    def test_33_percent(self):
        """Perform the ecdf test on the 33 percent data."""
        self._test_ecdf(self.frame_33_percent, "predicted")

    def test_1_2_5(self):
        """ Perform the ecdf test on the 1 2 5 data  """
        self._test_ecdf(self.frame_netf, "rating")

    def _test_ecdf(self, frame, predict_column):
        """Test ecdf on the frame, predict on predict_column."""
        ecdf_lines = map(lambda l: l[0],
                         frame.take(frame.row_count, columns=predict_column))

        # calculate the values in python
        ecdf_Python = {ele[0]: ele[1]
                       for ele in self._process_ecdf(ecdf_lines)}

        # use the system to calculate the values
        frame_ecdf = frame.ecdf(predict_column)

        # extract the relevant pieces of the frame
        ecdf_IA = {ele[0]: ele[1]
                   for ele in frame_ecdf.take(frame_ecdf.row_count)}

        self.assertEqual(ecdf_IA.keys(), ecdf_Python.keys())
        for key in ecdf_IA.keys():
            self.assertAlmostEqual(ecdf_IA[key], ecdf_Python[key],
                                   delta=0.0001)

    def _process_ecdf(self, distribution_list):
        """Calculate the ECDF in python locally."""
        total_count = len(distribution_list)

        # get keys of the distribution
        key_list = sorted(list(set(distribution_list)))

        # Associate each element with the percent of times it appears
        # in the list
        cdf = map(lambda key:
                  (key, distribution_list.count(key) / float(total_count)),
                  key_list)

        current_count = 0.0
        return_list = []

        # construct the cumulative sum
        for (element, length) in cdf:
            current_count += length
            return_list.append([element, current_count])

        return return_list

    def test_ecdf_bad_name(self):
        """Test ecdf with an invalid column name."""
        with(self.assertRaises((ia.rest.command.CommandServerError,
                                RuntimeError))):
            self.frame_netf.ecdf("bad_name")

    def test_ecdf_bad_type(self):
        """Test ecdf with an invalid column type."""
        with(self.assertRaises((ia.rest.command.CommandServerError,
                                RuntimeError))):
            self.frame_netf.ecdf(5)

    def test_ecdf_none(self):
        """Test ecdf with a None for the column name."""
        with(self.assertRaises((ia.rest.command.CommandServerError,
                                RuntimeError))):
            self.frame_netf.ecdf(None)

if __name__ == '__main__':
    unittest.main()
