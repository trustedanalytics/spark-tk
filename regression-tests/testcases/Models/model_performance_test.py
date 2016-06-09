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
# Author: Venkatesh Bharadwaj
"""
usage:
python2.7 model_performance.py

Tests model performance computations (precision, accuracy, recall and
fmeasure). This test replicates model performance and then compares the
results with known values.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class ModelPerformanceTest(atk_test.ATKTestCase):

    def setUp(self):
        """
        Verify that the input and baselines exist before running the tests.
        """
        super(ModelPerformanceTest, self).setUp()

        # datasets
        self.model_33_percent = "model_33_percent.csv"
        self.model_33_50 = "model_33_50.csv"
        self.model_color = "model_color.csv"

        # The following values are calculated manually for the given
        # datasets, these values are used for comparison of the results
        # generated from the actual execution.
        self.data_33_percent_accuracy = 0.333282318130803
        self.data_33_percent_precision = 1.0
        self.data_33_percent_recall = 0.333282318130803
        self.data_33_percent_fmeasure = 0.499942605701167

        self.data_33_50_percent_accuracy = 0.5
        self.data_33_50_percent_precision = 0.49996174152574796
        self.data_33_50_percent_recall = 0.66673469387755102
        self.data_33_50_percent_fmeasure = 0.664539926229

        self.data_colors_multiclass_accuracy = 0.25059999999999999
        self.data_colors_multiclass_precision = 0.25061258131995501
        self.data_colors_multiclass_recall = 0.25059999999999999
        self.data_colors_multiclass_fmeasure = 0.25060485758149481

        self.schema1 = [("user_id", ia.int32), ("vertex_type", str),
                        ("movie_id", ia.int32), ("rating", ia.int32),
                        ("splits", str), ("predicted", ia.int32)]
        self.schema2 = [("color1", str), ("predicted", str)]

        # Movie user data with original ratings
        self.frame1 = frame_utils.build_frame(
            self.model_33_percent, self.schema1, self.prefix)
        # Movie user data with some missing ratings
        self.frame2 = frame_utils.build_frame(
            self.model_33_50, self.schema1, self.prefix)
        # Movie user data with some missing ratings
        self.frame3 = frame_utils.build_frame(
            self.model_color, self.schema2, self.prefix)

    def test_frame_33_percent_accuracy(self):
        """Tests the 33 percent model accuracy, recall, precision, fmeasure"""
        # classification_metrics for the file with binary ratings and 33%
        # correct ratings is calculated and then compared with the known
        # results for this dataset.
        cm1 = self.frame1.classification_metrics('rating', 'predicted',
                                                 pos_label=1)

        self.assertAlmostEquals(self.data_33_percent_accuracy,
                                cm1.accuracy)
        self.assertAlmostEquals(self.data_33_percent_precision,
                                cm1.precision)
        self.assertAlmostEquals(self.data_33_percent_recall,
                                cm1.recall)
        self.assertAlmostEquals(self.data_33_percent_fmeasure,
                                cm1.f_measure)

    def test_frame_33_50_percent_accuracy(self):
        """Tests the 33-50 percents accuracy, precision, recall, fmeasure."""
        # classification_metrics for the file with binary ratings and 33%
        # ratings and 50% binary data distribution is calculated and then
        # compared with the known results for this dataset.
        cm2 = self.frame2.classification_metrics('rating', 'predicted',
                                                 pos_label=1, beta=10)

        self.assertAlmostEquals(self.data_33_50_percent_accuracy,
                                cm2.accuracy)
        self.assertAlmostEquals(self.data_33_50_percent_precision,
                                cm2.precision)
        self.assertAlmostEquals(self.data_33_50_percent_recall,
                                cm2.recall)
        self.assertAlmostEquals(self.data_33_50_percent_fmeasure,
                                cm2.f_measure)

    def test_frame_colors_multiclass_accuracy(self):
        """Tests the colors multiclass accuracy, precision, recall, fmeasure"""
        # classification_metrics for the file with multiple classes
        # (4 colors) and random predictions are calculated and then compared
        # with the known results for this dataset.
        cm3 = self.frame3.classification_metrics('color1', 'predicted')

        self.assertAlmostEquals(self.data_colors_multiclass_accuracy,
                                cm3.accuracy)
        self.assertAlmostEquals(self.data_colors_multiclass_precision,
                                cm3.precision)
        self.assertAlmostEquals(self.data_colors_multiclass_recall,
                                cm3.recall)
        self.assertAlmostEquals(self.data_colors_multiclass_fmeasure,
                                cm3.f_measure)


if __name__ == '__main__':
    unittest.main()
