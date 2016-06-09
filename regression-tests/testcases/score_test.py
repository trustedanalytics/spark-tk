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
"""
   Usage:  python2.7 svm_score_test.py
   Test SVM scoring.

Functionality tested:
Model with wide gutters
Model with narrow gutters
Model with sparse population on one side.
Model in 1-D, 2-D, many-D
Model with built-in errors (hyperplane is insufficient)

Simple positive & negative classifications
Data within gutter, either side of path
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.03.20"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append("/home/wdwickar/libsvm-3.20/python")

import unittest

# from svm import *
# from svmutil import *

import trustedanalytics as ia

from qalib import common_utils
from qalib import frame_utils
from qalib import atk_test


class SvmScoreTest(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(SvmScoreTest, self).setUp()

        self.train_file = "LogReg_all_1.csv"
        self.train_schema = [("data", ia.int32),
                             ("label", ia.int32)]

        self.square_file = "emp.csv"
        self.square_schema = [("idNum", ia.float64),
                              ("c2", str),
                              ("c3", ia.int32),
                              ("c4", str),
                              ("c5", ia.int32),
                              ("c6", str),
                              ]

        self.shuttle_file_all = "shuttle_scale_cut"
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
        print "Build empty model"
        # self.libsvm_model = ia.LibsvmModel(
        #     common_utils.get_a_name(self.prefix))
        self.libsvm_model = ia.LibsvmModel(
            common_utils.get_a_name(self.prefix))

        # Use this line to rebuild the Libsvm model for scoring testing.
        # self.libsvm_model = ia.LibsvmModel("Shuttle_LibSVM_Model")

        # self.host_ip = socket.gethostbyname_ex(ia.server.host)[2][0]
        # self.service_url = 'http://10.54.8.187:9099/v1/models/' \
        #     + str(self.libsvm_model._id) + '/score'
        # self.service_url = \
        #     'http://' + \
        #     str(self.host_ip) + ':' + str(ia.server.port) + \
        #     '/v1/models/' + str(self.libsvm_model._id) + '/score'
        # print "Model ID", self.libsvm_model._id
        # print "Service URL", self.service_url

    def test_svm_score(self):
        """
        Verify that LibSVM (open source) and LibsvmModel (ATK)
          operate the same on a small 1-class model.
        """
        # Test set is a 3x3 square lattice of points.
        # Model should be a simple circle.

        spread = 1.25    # beyond limit of learned model

        # Direct call to LibSVM.
        # y = 9*[1]
        block_data = [[-1, -1, 1],
                      [-1,  0, 1],
                      [-1,  1, 1],
                      [0,  -1, 1],
                      [0,   0, 1],
                      [0,   1, 1],
                      [1,  -1, 1],
                      [1,   0, 1],
                      [1,   1, 1]]
        block_schema = [('tr_row', ia.float64),
                        ('tr_col', ia.float64),
                        ('pos_one', ia.float64)]

        # Build a square lattice from a file with the
        # numbers 1-9 in the first column.
        # square_model = ia.LibsvmModel("Square3_LibSVM_Model")

        square_frame = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        print square_frame.inspect()

        self.libsvm_model.train(square_frame, u"pos_one", ["tr_row", "tr_col"],
                                svm_type=2,     # 1-class model
                                epsilon=10e-3,  # get only one outlier
                                gamma=1.0/2,
                                nu=0.1,
                                p=0.1)

        # Test against the original training data;
        #   this should match almost perfectly.
        # Modeling allows for one point classified as an outlier.
        predicted_frame = self.libsvm_model.predict(square_frame, ["tr_row", "tr_col"])
        pd_frame = predicted_frame.download(predicted_frame.row_count)
        num_errors = 0
        for _, i in pd_frame.iterrows():
            scored = self.libsvm_model.score([i["tr_row"], i["tr_col"]])
            print scored     # Get a proper view of the new return format.
            if scored != i["pos_one"]:
                num_errors += 1
        
        self.assertLessEqual(num_errors, 1)

    # @unittest.skip("API change; test case update in progress")
    def test_3by3_square(self):
        """
        Verify that LibSVM (open source) and LibsvmModel (ATK)
          operate the same on a small 1-class model.
        """
        # Test set is a 3x3 square lattice of points.
        # Model should be a simple circle.

        spread = 1.25    # beyond limit of learned model

        # Direct call to LibSVM.
        # y = 9*[1]
        block_data = [[-1, -1, 1],
                      [-1,  0, 1],
                      [-1,  1, 1],
                      [0,  -1, 1],
                      [0,   0, 1],
                      [0,   1, 1],
                      [1,  -1, 1],
                      [1,   0, 1],
                      [1,   1, 1]]
        block_schema = [('tr_row', ia.float64),
                        ('tr_col', ia.float64),
                        ('pos_one', ia.float64)]

        # Build a square lattice from a file with the
        # numbers 1-9 in the first column.
        # square_model = ia.LibsvmModel("Square3_LibSVM_Model")

        square_frame = frame_utils.build_frame(
            block_data, block_schema, self.prefix, file_format="list")
        print square_frame.inspect()

        self.libsvm_model.train(square_frame, "pos_one", ["tr_row", "tr_col"],
                                svm_type=2,     # 1-class model
                                epsilon=10e-3,  # get only one outlier
                                gamma=1.0/2,
                                nu=0.1,
                                p=0.1)

        # Test against the original training data;
        #   this should match almost perfectly.
        # Modeling allows for one point classified as an outlier.
        predicted_frame = self.libsvm_model.predict(square_frame)

        test_obj = self.libsvm_model.test(square_frame, "pos_one",
                                          ["tr_row", "tr_col"])
        print "Vs training data", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "1-class square model failed accuracy check.")

        # Check against a spread-out data set
        # All points except the center should be outliers.
        outer_square = square_frame.copy()
        outer_square.add_columns(lambda row: (
            row.tr_row + spread*row.tr_row,
            row.tr_col + spread*row.tr_col,
            1 if row.tr_row == 0 and row.tr_col == 0
            else -1), [('te_row', ia.float64),
                       ('te_col', ia.float64),
                       ('neg_one', ia.float64)]
        )
        predicted_frame = self.libsvm_model.predict(outer_square,
                                                    ["te_row", "te_col"])
        print predicted_frame.inspect()

        test_obj = self.libsvm_model.test(outer_square, "neg_one",
                                          ["te_row", "te_col"])
        print "Vs outer data", test_obj
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0,
                                "1-class square model failed accuracy check.")

    # @unittest.skip("API change; test case update in progress")
    def test_shuttle(self):
        """
        Test with Kathleen's 'shuttle scale cut' data
        """
        # import numpy as np

        # ia.errors.show_details = True
        # ia.loggers.set_api()

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

        self.libsvm_model.train(train_frame, "class", observe_list,
                                svm_type=2,     # 1-class model
                                epsilon=10e-8,  # from Kathleen
                                degree=3,
                                gamma=10e-2,    # from Kathleen
                                coef=0.0,
                                nu=10e-4,       # from Kathleen
                                cache_size=100.0,
                                shrinking=1,
                                probability=0,
                                nr_weight=0,
                                c=1.0,
                                p=0.1
                                )

        # Build testing frame and test the model
        test_frame = frame_utils.build_frame(
            self.shuttle_file_te, [("input_line", str)], prefix=self.prefix)

        test_frame.add_columns(split_input_line, self.shuttle_schema)
        test_frame.drop_columns("input_line")

        test_result = self.libsvm_model.test(test_frame, "class", observe_list)
        print test_result
        matrix = test_result.confusion_matrix
        tn = matrix['Predicted_Neg']['Actual_Neg']
        fp = matrix['Predicted_Pos']['Actual_Neg']
        fn = matrix['Predicted_Neg']['Actual_Pos']
        tp = matrix['Predicted_Pos']['Actual_Pos']
        total_row_count = tn + fp + fn + tp
        self.assertLess((fp+fn)/total_row_count, 0.01)

        # Build prediction frame and use the model to forecast
        pred_frame = frame_utils.build_frame(
            self.shuttle_file_va, [("input_line", str)], prefix=self.prefix)

        pred_frame.add_columns(split_input_line, self.shuttle_schema)
        pred_frame.drop_columns("input_line")

        forecast = self.libsvm_model.predict(pred_frame, observe_list)
        forecast.drop_rows(lambda row: row['class'] == row['predicted_label'])
        self.assertLess(forecast.row_count / total_row_count, 0.01)


if __name__ == "__main__":
    unittest.main()
