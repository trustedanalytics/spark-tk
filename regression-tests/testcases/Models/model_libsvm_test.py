##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015, 2016 Intel Corporation All Rights Reserved.
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
""" Usage:  python2.7 model_libsvm_test.py """

# We do not need to stress the functionality of SVM;
#   it's an Apache implementation.
# Just the interface and basic capabilities are enough.

# Functionality tested:
# Model in 1-D, 2-D, many-D
# Model with built-in errors (hyperplane is insufficient)
#
# Simple positive & negative classifications
# Data within gutter, either side of path
# All data within class (useless model)
#
# Negative testing
#   invalid classification (not 0 / 1)

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2016.02.02"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import common_utils
from qalib import frame_utils
from qalib import atk_test


class LibvmModelTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(LibvmModelTest, self).setUp()

        self.shuttle_file_tr = "shuttle_scale_cut_tr"
        self.shuttle_file_te = "shuttle_scale_cut_te"
        self.shuttle_file_va = "shuttle_scale_cut_va"
        self.shuttle_schema = [("class", ia.float64),
                               ("col1", ia.float64),
                               ("col2", ia.float64),
                               ("col3", ia.float64),
                               ("col4", ia.float64),
                               ("col5", ia.float64),
                               ("col6", ia.float64),
                               ("col7", ia.float64),
                               ("col8", ia.float64),
                               ("col9", ia.float64)]
        self.shuttle_headers = {'Content-type': 'application/json',
                                'Accept': 'application/json,text/plain',
                                'Authorization': "test_api_key_1"}

        # ia.loggers.set_http()

    def lattice2frame(self, matrix):
        """Convert 2D string lattice to data frame."""
        # The input matrix is a string lattice with data points marked
        #   with + and - (2-class model), or with integers (multi-class).
        #   Any other characters are ignored.
        # The lattice's center is taken as the origin.
        #
        # return: Frame with the positions and values converted to
        #   SVM input requirements.
        # This frame is ready as input to train, test, or predict.

        block_data = []
        schema = [('x', ia.float64),
                  ('y', ia.float64),
                  ('model_class', ia.int32)]

        # Grabbing center column from center row allows for a skew matrix,
        #   so long as the center row is complete.
        origin_y = len(matrix)/2
        origin_x = len(matrix[origin_y])/2

        # print "L2M TRACE", matrix, "origin at", origin_x, origin_y
        for y in range(len(matrix)):
            for x in range(len(matrix[y])):
                svm_class = None
                char = matrix[y][x]
                if char == '+' or char == '1':
                    svm_class = 1
                elif char == '-' or char == '0':
                    svm_class = -1
                elif char.isdigit():
                    svm_class = int(char)
                if svm_class is not None:
                    block_data.append([x-origin_x, origin_y-y, svm_class])
        block_data.sort()
        # print "L2M TRACE", block_data

        if len(block_data) == 0:
            frame = None
        else:
            frame = frame_utils.build_frame(
                block_data, schema, self.prefix, file_format="list")
        return frame

    def validate_lattice2frame(self):
        """
        Verify that lattice2frame operates as expected.
        """

        train_lattice = ["012",
                         "345",
                         "678"]

        training_frame = self.lattice2frame(train_lattice)
        print training_frame.inspect(training_frame.row_count)

    def test_simple_line(self):
        """
        Verify that LibsvmModel operates as expected.
        """
        # Test set is a 3x3 square lattice of points
        #   with a fully accurate, linear, unbiased divider.

        train_lattice = ["+++",
                         "++-",
                         "---"]

        test_lattice = ["+ + +",
                        "+   +",
                        "+ + -",
                        "+   -",
                        "- - -"]

        training_frame = self.lattice2frame(train_lattice)
        # training_frame = self.lattice2frame(test_lattice)
        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        print training_frame.inspect(training_frame.row_count)

        svm_model.train(training_frame,
                        "model_class", ["x", "y"],
                        svm_type=0,     # C-SVC model
                        kernel_type=0   # linear
                        )

        # Test against the original training data;
        #   this should match almost perfectly.
        print "\n--- PREDICT AGAINST TRAINING SET ---"
        predicted_frame = svm_model.predict(training_frame)
        print predicted_frame.inspect(predicted_frame.row_count)

        test_obj = svm_model.test(training_frame, "model_class",
                                  ["x", "y"])
        print test_obj
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "1-class square model failed self-check.")

        print "\n--- PREDICT AGAINST EXTRAPOLATION SET ---"
        outer_square = self.lattice2frame(test_lattice)
        predicted_frame = svm_model.predict(outer_square,
                                            ["x", "y"])
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.filter(
            lambda row: row.model_class == row.predicted_label)

        test_obj = svm_model.test(outer_square, "model_class",
                                  ["x", "y"])
        print "Vs test data\n", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "1-class square model failed accuracy check.")

    def test_multi_dim(self):
        """
        Verify that LibsvmModel operates as expected in 3 dimensions.
        """
        # Test set is a 3D model on the plane x + y - 2z > 0

        train_data = [  # x + y - 2z >= 0
            (0, 0, 0, 1),
            (1, 1, 1, 1),
            (1, 1, 4, -1),
            (2, 3, 3, -1),
            (2, 3, 2, 1),
            (2, 0, 0, 1),
            (3, 0, 2, -1),
            (3, 6, 0, 1),
            (3, -4, 0, -1),
            (50, 50, 49, 1),
            (50, 50, 51, -1),
            (-50, -50, -49, -1),
            (-50, -50, -51, 1),
        ]

        test_data = [
            (0, 0, 1, -1),
            (-1, -1, 1, -1),
            (-1, -2, -1, -1),
            (100, 100, 95, 1),
            (-100, -100, -95, -1),
            (2, 2, 1, 1)
        ]

        schema = [('d1', ia.float64),
                  ('d2', ia.float64),
                  ('d3', ia.float64),
                  ('model_class', ia.int32),
                  ]
        training_frame = frame_utils.build_frame(
            train_data, schema, self.prefix, file_format="list")
        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        print training_frame.inspect(training_frame.row_count)

        svm_model.train(training_frame,
                        "model_class", ["d1", "d2", "d3"],
                        svm_type=0,     # C-SVC model
                        kernel_type=0   # linear
                        )

        # Test against the original training data;
        #   this should match almost perfectly.
        print "\n--- PREDICT AGAINST TRAINING SET ---"
        predicted_frame = svm_model.predict(training_frame)
        print predicted_frame.inspect(predicted_frame.row_count)

        test_obj = svm_model.test(training_frame, "model_class",
                                  ["d1", "d2", "d3"])
        print test_obj
        self.assertGreaterEqual(test_obj.accuracy, 0.75,
                                "3D model failed self-check.")

        print "\n--- PREDICT AGAINST EXTRAPOLATION SET ---"
        outer_square = frame_utils.build_frame(
            test_data, schema, self.prefix, file_format="list")
        predicted_frame = svm_model.predict(outer_square,
                                            ["d1", "d2", "d3"])
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.drop_rows(
            lambda row: row.model_class == row.predicted_label)
        print "BAD_ROWS:", predicted_frame.inspect(predicted_frame.row_count)

        test_obj = svm_model.test(outer_square, "model_class",
                                  ["d1", "d2", "d3"])
        print "Vs test data\n", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 0.75,
                                "3D model failed accuracy check.")

    def test_fuzzy_line(self):
        """
        Verify that LibsvmModel operates as expected.
        """
        # Test set is a lattice of points, a few outliers, large gap.

        train_lattice = ["++   --",
                         "++    +",
                         "++    -",
                         "+-+  --",
                         "++    -",
                         "+    --",
                         "++    -",
                         ]

        test_lattice = ["++++---",
                        "++++---",
                        "++++---",
                        "++++---",
                        "+++----",
                        "+++----",
                        "+++----",
                        ]

        training_frame = self.lattice2frame(train_lattice)
        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        print training_frame.inspect(training_frame.row_count)

        svm_model.train(training_frame,
                        "model_class", ["x", "y"],
                        svm_type=0,     # C-SVC model
                        kernel_type=0   # linear
        )

        # Test against the original training data;
        #   this should match almost perfectly.
        print "\n--- PREDICT AGAINST TRAINING SET ---"
        predicted_frame = svm_model.predict(training_frame)
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.drop_rows(
            lambda row: row.model_class == row.predicted_label)
        print "BAD ROWS:\n", predicted_frame.inspect(predicted_frame.row_count)

        test_obj = svm_model.test(training_frame, "model_class",
                                  ["x", "y"])
        print "Vs test data\n", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "fuzzy line model failed self-check, accuracy %f" % test_obj.accuracy)

        print "\n--- PREDICT AGAINST EXTRAPOLATION SET ---"
        outer_square = self.lattice2frame(test_lattice)
        predicted_frame = svm_model.predict(outer_square,
                                            ["x", "y"])
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.drop_rows(
            lambda row: row.model_class == row.predicted_label)
        print "BAD ROWS:\n", predicted_frame.inspect(predicted_frame.row_count)

        test_obj = svm_model.test(outer_square, "model_class",
                                  ["x", "y"])
        print "Vs test data\n", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 0.9,
                                "fuzzy line model failed accuracy check, accuracy %f" % test_obj.accuracy)

    def test_biased_line(self):
        """
        Verify that LibsvmModel operates as expected.
        """
        # Test set is a lattice of points, a few outliers, large gap.

        train_lattice = ["+++++++",
                         "+++++++",
                         "+++++++",
                         "++++++-",
                         "+++++--",
                         "++++---",
                         "+++----"
                         ]

        training_frame = self.lattice2frame(train_lattice)
        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        print training_frame.inspect(training_frame.row_count)

        svm_model.train(training_frame,
                        "model_class", ["x", "y"],
                        svm_type=0,     # C-SVC model
                        kernel_type=0   # linear
                        )

        # Test against the original training data;
        #   this should match almost perfectly.
        print "\n--- PREDICT AGAINST TRAINING SET ---"
        predicted_frame = svm_model.predict(training_frame)
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.drop_rows(
            lambda row: row.model_class == row.predicted_label)
        print "BAD ROWS:\n", predicted_frame.inspect(predicted_frame.row_count)

        test_obj = svm_model.test(training_frame, "model_class",
                                  ["x", "y"])
        print test_obj

        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "biased line model failed self-check.")

    def test_all_in_class(self):
        """
        Verify that LibsvmModel operates as expected.
        """
        # Test set is a 3x3 square lattice of points, all in the model.
        # Validate that remote points are outside the model.

        train_lattice = ["+++",
                         "+++",
                         "+++"]

        test_lattice = ["-                   -         -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-                             -",
                        "-              -              -"
                        ]

        training_frame = self.lattice2frame(train_lattice)
        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        print training_frame.inspect(training_frame.row_count)

        svm_model.train(training_frame,
                        "model_class", ["x", "y"],
                        svm_type=2,     # 1-class model
                        degree=3
        )

        print "\n--- PREDICT AGAINST EXTRAPOLATION SET ---"
        outer_square = self.lattice2frame(test_lattice)
        predicted_frame = svm_model.predict(outer_square,
                                            ["x", "y"])
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.filter(
            lambda row: row.model_class == row.predicted_label)

        test_obj = svm_model.test(outer_square, "model_class",
                                  ["x", "y"])
        print "Vs test data\n", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "all-in model failed accuracy check.")

    @unittest.skip("DPNG-4335")
    def test_shuttle(self):
        """
        Test with Kathleen's 'shuttle scale cut' data
        """
        # import numpy as np

        # ia.errors.show_details = True
        # ia.loggers.set_api()

        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        line_len = len(self.shuttle_schema)
        observe_list = ["col"+str(i) for i in range(1, 10)]

        def split_input_line(row):
            """Extract features and values from input line in vector format"""
            data = row['input_line'].split(' ')
            full_line = line_len * [0.0]
            full_line[0] = float(data[0])
            for index, value in [feature.split(':') for feature in data[1:-1]]:
                full_line[int(index)] = float(value)
            return full_line

        def extract_vector_from_input(row):
            """Extract features and values from input line in SVM format"""
            data = row['input_line'].split(' ')
            full_line = (line_len-1) * [0.0]
            for index, value in [feature.split(':') for feature in data[1:-1]]:
                full_line[int(index)-1] = float(value)
            return full_line

        # Build training frame and train the model
        train_frame = frame_utils.build_frame(
            self.shuttle_file_tr, [("input_line", str)], prefix=self.prefix)

        train_frame.add_columns(split_input_line, self.shuttle_schema)
        train_frame.drop_columns("input_line")

        svm_model.train(train_frame, "class", observe_list,
                        epsilon=0.000001,
                        degree=3,
                        gamma=0.11,
                        coef=0.0,
                        nu=0.0001,
                        cache_size=100.0,
                        shrinking=1,
                        probability=0,
                        c=1.0,
                        p=0.1,
                        nr_weight=0
        )

        # Build testing frame and test the model
        test_frame = frame_utils.build_frame(
            self.shuttle_file_te, [("input_line", str)], prefix=self.prefix)

        test_frame.add_columns(split_input_line, self.shuttle_schema)
        test_frame.drop_columns("input_line")

        test_result = svm_model.test(test_frame, "class", observe_list)
        print test_result
        matrix = test_result.confusion_matrix
        true_neg = matrix['Predicted_Neg']['Actual_Neg']
        false_pos = matrix['Predicted_Pos']['Actual_Neg']
        false_neg = matrix['Predicted_Neg']['Actual_Pos']
        true_pos = matrix['Predicted_Pos']['Actual_Pos']
        total_row_count = true_neg + false_pos + false_neg + true_pos
        self.assertLess(float(false_pos+false_neg)/total_row_count, 0.02)

        # Build prediction frame and use the model to forecast
        pred_frame = frame_utils.build_frame(
            self.shuttle_file_va, [("input_line", str)], prefix=self.prefix)

        pred_frame.add_columns(split_input_line, self.shuttle_schema)
        pred_frame.drop_columns("input_line")

        forecast = svm_model.predict(pred_frame, observe_list)
        total_row_count = forecast.row_count
        forecast.drop_rows(lambda row: row['class'] == row['predicted_label'])
        self.assertLess(float(forecast.row_count) / total_row_count, 0.02)

    def test_3class_linear(self):
        """
        Verify that 3-class model is legal.
        NOTE: This is *not* legal with Apache's ML SVM.
        """
        # Test set is a square lattice of points
        #   with two fully accurate, linear, unbiased dividers.

        train_lattice = ["1  1  1  1",
                         "          ",
                         "1  1  13  ",
                         "22   3    ",
                         "2  3     3",
                         "3  3     3"]

        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        training_frame = self.lattice2frame(train_lattice)

        svm_model.train(training_frame,
                        "model_class", ["x", "y"],
                        svm_type=0,    # C-SVC model
                        kernel_type=0  # linear
        )
        predicted_frame = svm_model.predict(training_frame, ["x", "y"])
        print predicted_frame.inspect(predicted_frame.row_count)
        predicted_frame.drop_rows(
            lambda row: row.model_class == row.predicted_label)
        print predicted_frame.inspect(predicted_frame.row_count)

        # The model makes 3 classification errors in 17 tries;
        #   perfection is possible, but libsvm misses it.
        self.assertLessEqual(predicted_frame.row_count, 3)

        test_obj = svm_model.test(training_frame, "model_class")
        print test_obj

    def test_bad_col_name(self):
        """
        Verify that a bad column name raises an exception.
        """
        # Test set is a square lattice of points
        #   with a fully accurate, linear, unbiased divider.

        train_lattice = ["1  1  1  1",
                         "          ",
                         "1  1  10  ",
                         "1         ",
                         "         0",
                         "0        0"]

        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        training_frame = self.lattice2frame(train_lattice)

        print "Try a bad classification column"
        self.assertRaises(ia.rest.command.CommandServerError,
                          svm_model.train,
                          training_frame,
                          "no_such_col", ["x", "y"])

        print "Try a bad data column"
        self.assertRaises(ia.rest.command.CommandServerError,
                          svm_model.train,
                          training_frame,
                          "model_class", ["no_such_col", "y"])

        print "Try a null data column"
        self.assertRaises(ia.rest.command.CommandServerError,
                          svm_model.train,
                          training_frame,
                          "model_class", ["x", None])

    @unittest.skip("DPNG-4335")
    def test_bad_data(self):
        """
        Verify that invalid models raise exceptions.
        """
        # Empty training set

        train_lattice = ["1  1  1  1",
                         "          ",
                         "1  1  10  ",
                         "1         ",
                         "         0",
                         "0        0"]

        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        training_frame = self.lattice2frame(train_lattice)
        training_frame.drop_rows(lambda row: True)
        print training_frame.row_count, "rows left"
        svm_model.train(training_frame,
                        "model_class", ["x", "y"])

        self.assertRaises(ia.rest.command.CommandServerError,
                          svm_model.train,
                          training_frame,
                          "model_class", ["x", "y"])

        # Degenerate training set

        train_lattice = ["0"]

        svm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))
        training_frame = self.lattice2frame(train_lattice)

        svm_model.train(training_frame,
                        "model_class", ["x", "y"])


if __name__ == "__main__":
    unittest.main()
