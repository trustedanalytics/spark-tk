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
__version__ = "2016.03.02"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import random

import trustedanalytics as ta

from qalib import common_utils
from qalib import frame_utils
from qalib import atk_test


class SvmModelTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(SvmModelTest, self).setUp()
        self.same_old_seed = 10039492

    def test_gmm_basic(self):
        """
        Verify that model operates as expected in straightforward case.
        Verify return values format
        """
        # Validate basic return structure

        block_data = [
            [4, 'a'],
            [5, 'b'],
            [5, 'c'],
            [6, 'd'],
            [5, 'e'],
            [6, 'f'], [6, ''], [6, ''], [6, ''],
            [7, 'g'], [7, ''], [7, ''], [7, ''], [7, ''], [7, ''],
            [8, 'h'], [8, ''], [8, ''], [8, ''],
            [9, 'i']
        ]
        frame = ta.Frame(ta.UploadRows(
            block_data,
            [("data", ta.float64), ("name", str)]))
        print frame.inspect(frame.row_count)
        model = ta.GmmModel()
        train_output = model.train(frame, ["data"],
                                   column_scalings=[1.0],
                                   k=2,
                                   max_iterations=1000,
                                   convergence_tol=1e-5,
                                   seed=self.same_old_seed)       # Train on normal seed
        print train_output
        prediction = model.predict(frame, ["data"])
        prediction.sort(['predicted_cluster'])
        print prediction.inspect(prediction.row_count)

        print "\nTRAINING RESULT:"
        for k, v in train_output.iteritems():
            print k, type(v), v

        print "\nCLUSTER_SIZE:"
        for k, v in train_output["cluster_size"].iteritems():
            print k, type(v), v

        # Fails on DPNG-5435, structure of return value.
        print "\nGAUSSIANS:"
        # for k, v in train_output["gaussians"].iteritems():
        #     print k, type(v), v
        for row in train_output["gaussians"]:
            print row

        # DPNG-5345 Return structure incorrect, unusable
        # # Means should be 7 and 2.
        # # Sigmas should be 2 and 1/2.
        # # Cluster sizes should be 16 and 4.
        # census = train_output["cluster_size"].values()
        # self.assertTrue(16 in census)
        #
        # mean0 = train_output["gaussians"][0][0]
        # mean1 = train_output["gaussians"][1][0]
        # # self.assertIn(mean0, [2.0, 7.0])
        # # self.assertIn(mean1, [2.0, 7.0])
        #
        # sigma0 = train_output["gaussians"][0][1]
        # sigma1 = train_output["gaussians"][1][1]
        # # self.assertIn(sigma0, [0.5, 1.0])
        # # self.assertIn(sigma1, [0.5, 1.0])
        # print mean0, sigma0
        # print mean1, sigma1

    def test_gmm_multi_dim(self):
        """
        Verify model in multiple dimensions
        """

        block_data = []

        # build circle around 0,0
        random.seed(self.same_old_seed)

        large_pop = 200
        large_ctr = (0, 0, 0)
        large_sd = (10.0, 10.0, 10.0)

        for _ in range(large_pop):
            x = random.gauss(large_ctr[0], large_sd[0])
            y = random.gauss(large_ctr[1], large_sd[1])
            z = random.gauss(large_ctr[2], large_sd[2])
            block_data.append([x, y, z])

        tiny_pop = 40
        tiny_ctr = (1, 2, 1)
        tiny_sd = (0.5, 0.3, 0.1)

        for _ in range(tiny_pop):
            x = random.gauss(tiny_ctr[0], tiny_sd[0])
            y = random.gauss(tiny_ctr[1], tiny_sd[1])
            z = random.gauss(tiny_ctr[2], tiny_sd[2])
            block_data.append([x, y, z])

        mid_pop = 80
        mid_ctr = (-2, -2, -2)
        mid_sd = (1.0, 1.0, 1.0)

        for _ in range(mid_pop):
            x = random.gauss(mid_ctr[0], mid_sd[0])
            y = random.gauss(mid_ctr[1], mid_sd[1])
            z = random.gauss(mid_ctr[2], mid_sd[2])
            block_data.append([x, y, z])

        frame = ta.Frame(ta.UploadRows(
            block_data,
            [("x", ta.float64),
             ("y", ta.float64),
             ("z", ta.float64)]))
        # print frame.inspect(frame.row_count)
        model = ta.GmmModel()
        train_output = model.train(frame, ["x", "y", "z"],
                                   column_scalings=[1.0, 1.0, 1.0],
                                   k=3,
                                   seed=self.same_old_seed)       # Train on normal seed
        print train_output
        prediction = model.predict(frame)
        prediction.sort(['x', 'predicted_cluster'])
        # print prediction.inspect(prediction.row_count)
        #
        # print "\nTRAINING RESULT:"
        # for k, v in train_output.iteritems():
        #     print k, type(v), v
        #
        # print "\nCLUSTER_SIZE:"
        # for k, v in train_output["cluster_size"].iteritems():
        #     print k, type(v), v

        # Fails on DPNG-5435, structure of return value.
        print "\nGAUSSIANS:"
        # for k, v in train_output["gaussians"].iteritems():
        #     print k, type(v), v
        for row in train_output["gaussians"]:
            print row

        # Results are not yet ready for prime time, with lists returned as character strings.
        census = sorted([train_output["cluster_size"]["Cluster:" + str(i)] for i in range(3)])
        print census
        expected = [40, 80, 200]
        for cluster in range(3):
            self.assertAlmostEqual(float(census[cluster])/expected[cluster], 1.0, 1)

    def test_gmm_degenerate_cases(self):
        """
        Verify that model operates as expected in degenerate cases.
        There are no assertions; completion is sufficient evidence.
        """
        # 1 class
        # 1 iteration
        # Insanely liberal convergence
        # Negative, reasonable seed

        block_data = [
            [4, 'a'],
            [5, 'b'],
            [5, 'c'],
            [6, 'd'],
            [5, 'e'],
            [6, 'f'], [6, ''], [6, ''], [6, ''],
            [7, 'g'], [7, ''], [7, ''], [7, ''], [7, ''], [7, ''],
            [8, 'h'], [8, ''], [8, ''], [8, ''],
            [9, 'i']
        ]
        frame = ta.Frame(ta.UploadRows(
            block_data,
            [("data", ta.float64), ("name", str)]))
        print frame.inspect(frame.row_count)
        model = ta.GmmModel()

        # Train on 1 class only
        train_output = model.train(frame, ["data"],
                                   column_scalings=[1.0],
                                   k=1)
        print train_output

        # Train on 1 iteration only
        train_output = model.train(frame, ["data"],
                                   column_scalings=[1.0],
                                   max_iterations=1)
        print train_output

        # Train on high convergence
        train_output = model.train(frame, ["data"],
                                   column_scalings=[1.0],
                                   convergence_tol=1e6)
        print train_output

        # Train on negative seed
        train_output = model.train(frame, ["data"],
                                   column_scalings=[1.0],
                                   seed=-self.same_old_seed)
        print train_output

        # all-zero, negative, column scalings
        train_output = model.train(frame, ["data"],
                                   column_scalings=[0.0])
        print train_output

        train_output = model.train(frame, ["data"],
                                   column_scalings=[-1.0])
        print train_output

    def test_gmm_empty_frame(self):
        """
        Verify that model operates as expected in straightforward case.
        """
        # Train on an empty frame

        block_data = [
        ]
        frame = ta.Frame(ta.UploadRows(
            block_data,
            [("data", ta.float64), ("name", str)]))
        print frame.inspect(frame.row_count)
        model = ta.GmmModel()

        # Fails on DPNG-5462, exception & message
        # self.assertRaises(ValueError,
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train,
                          frame, ["data"],
                          column_scalings=[1.0])

    def test_gmm_error(self):
        """
        Verify that model operates as expected in degenerate cases.
        There are no assertions; completion is sufficient evidence.
        """
        # negative, 0 class
        # negative, 0 iteration
        # all-zero, negative, missing column scalings

        block_data = [
            [4, 'a'],
            [5, 'b'],
            [5, 'c'],
            [6, 'd'],
            [5, 'e'],
            [6, 'f'], [6, ''], [6, ''], [6, ''],
            [7, 'g'], [7, ''], [7, ''], [7, ''], [7, ''], [7, ''],
            [8, 'h'], [8, ''], [8, ''], [8, ''],
            [9, 'i']
        ]
        frame = ta.Frame(ta.UploadRows(
            block_data,
            [("data", ta.float64), ("name", str)]))
        print frame.inspect(frame.row_count)
        model = ta.GmmModel()

        # Train on 0 classes
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train, frame, ["data"],
                          column_scalings=[1.0],
                          k=0)

        # Train on negative classes
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train, frame, ["data"],
                          column_scalings=[1.0],
                          k=-5)

        # Train on 0 iterations
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train, frame, ["data"],
                          column_scalings=[1.0],
                          max_iterations=0)

        # Train on negative iterations
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train, frame, ["data"],
                          column_scalings=[1.0],
                          max_iterations=-20)

        # Insufficient column scalings
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train, frame, ["data"],
                          column_scalings=[])

        # Extra column scalings
        self.assertRaises(ta.rest.command.CommandServerError,
                          model.train, frame, ["data"],
                          column_scalings=[1.0, -2.0])

        # Missing column scalings
        self.assertRaises(TypeError,    # Missing required argument
                          model.train, frame, ["data"],
                          k=2)

    @unittest.skip("Test fails; may need more parameters exposed.")
    def test_gmm_wii_gesture(self):
        """
        Verify that model operates correctly on the Wii gesture example
        """

        train_file = "GmmTrainingData.csv"
        schema = [
            ("train_class", ta.int32),
            ("x_accel", ta.float64),
            ("y_accel", ta.float64),
            ("z_accel", ta.float64),
        ]
        train_frame = frame_utils.build_frame(train_file, schema, skip_header_lines=15)
        print train_frame.inspect()
        model = ta.GmmModel()
        train_output = model.train(train_frame,
                                   ["x_accel", "y_accel", "z_accel"],
                                   column_scalings=[1.0, 1.0, 1.0],
                                   k=5)
        print train_output
        prediction = model.predict(train_frame)
        prediction.sort(['train_class', 'x_accel'])
        print prediction.inspect(prediction.row_count)

        print "\nTRAINING RESULT:"
        for k, v in train_output.iteritems():
            print k, v

        print "\nCLUSTER_SIZE:"
        for k, v in train_output["cluster_size"].iteritems():
            print k, v

        # The largest class should have less than 25% of the observations.
        # Expected: class sizes are close to 300 each.
        # Actual: 1247 of 1526 observations wind up in one class.
        census = sorted([pop for pop in train_output["cluster_size"].itervalues()])
        print census
        self.assertLess(census[-1], sum(census)*0.25)

if __name__ == "__main__":
    unittest.main()
