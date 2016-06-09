##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2016 Intel Corporation All Rights Reserved.
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
""" Usage:  python2.7 model_gmm_test.py """

# We do not need to stress the functionality of GMM (Gaussian Mixture Model);
#   it's an Apache implementation.
# Just the interface and basic capabilities are enough.

# Functionality tested:
# Validate basic return structure
# Train on
#   empty frame
#   more classes than observations
# neg, 0, 1 classes
# neg, 0, 1 iterations
# Insanely liberal convergence
# Negative, reasonable seed


__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2016.02.25"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import time

import trustedanalytics as ta

from qalib import common_utils
from qalib import frame_utils
from qalib import atk_test


class SvmModelTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(SvmModelTest, self).setUp()

    def test_gmm_explore(self):
        """
        Verify that model handles large case.
        """

        schema = [["expected_class", ta.int32],
            ["f01", ta.float32],
            ["f02", ta.float32],
            ["f03", ta.float32],
            ["f04", ta.float32],
            ["f05", ta.float32],
            ["f06", ta.float32],
            ["f07", ta.float32],
            ["f08", ta.float32],
            ["f09", ta.float32],
            ["f10", ta.float32],
            ["f11", ta.float32],
            ["f12", ta.float32],
            ["f13", ta.float32],
            ["f14", ta.float32],
            ["f15", ta.float32],
            ["f16", ta.float32],
            ["f17", ta.float32],
            ["f18", ta.float32],
            ["f19", ta.float32],
            ["f20", ta.float32],
        ]
        col_list = [schema[col][0] for col in range(1, len(schema))]
        frame = frame_utils.build_frame("gmm_1Mrow_20Col.csv", schema)
        # frame = frame_utils.build_frame("gmm_100Krow_20Col.csv", schema)
        # print frame.inspect(frame.row_count)
        model = ta.GmmModel()
        train_start = time.time()
        train_output = model.train(frame, col_list,
                                   column_scalings=[1.0] * (len(schema)-1),
                                   k=10,
                                   seed=10039492)       # Train on normal seed
        print "Training took", time.time() - train_start, "sec."
        print train_output

        predict_start = time.time()
        prediction = model.predict(frame, col_list)
        print "Prediction took", time.time() - predict_start, "sec."

        # prediction.sort(['predicted_cluster'])
        # print prediction.inspect(prediction.row_count)

        print "\nTRAINING RESULT:"
        for k, v in train_output.iteritems():
            print k, type(v), v

        print "\nCLUSTER_SIZE:"
        for k, v in train_output["cluster_size"].iteritems():
            print k, type(v), v

        # Fails on DPNG-5435, structure of return value.
        # print "\nGAUSSIANS:"
        # for k, v in train_output["gaussians"].iteritems():
        #     print k, type(v), v


if __name__ == "__main__":
    unittest.main()
