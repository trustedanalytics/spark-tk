# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

""" Test model svm against known values"""
import unittest
from sparktkregtests.lib import sparktk_test


class SvmModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(SvmModelTest, self).setUp()
        train_lattice = ["1  1  1  1",
                         "          ",
                         "1  1  10  ",
                         "2         ",
                         "         0",
                         "0        0"]
        # used for error cases
        self.training_frame = self.lattice2frame(train_lattice)

        self.shuttle_file_tr = self.get_file("shuttle_scale_cut_tr")
        self.shuttle_file_te = self.get_file("shuttle_scale_cut_te")
        self.shuttle_file_va = self.get_file("shuttle_scale_cut_va")
        self.shuttle_schema = [("class", float),
                               ("col1", float),
                               ("col2", float),
                               ("col3", float),
                               ("col4", float),
                               ("col5", float),
                               ("col6", float),
                               ("col7", float),
                               ("col8", float),
                               ("col9", float)]

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
        schema = [('x', float),
                  ('y', float),
                  ('model_class', int)]

        # Grabbing center column from center row allows for a skew matrix,
        #   so long as the center row is complete.
        origin_y = len(matrix)/2
        origin_x = len(matrix[origin_y])/2

        # find the svm class for each item in the matrix
        # and append it to the block_data
        for y in range(len(matrix)):
            for x in range(len(matrix[y])):
                svm_class = None
                char = matrix[y][x]
                if char == '+' or char == '1':
                    svm_class = 1
                elif char == '-' or char == '0':
                    svm_class = 0
                elif char.isdigit():
                    svm_class = int(char)
                if svm_class is not None:
                    block_data.append([x-origin_x, origin_y-y, svm_class])
        block_data.sort()

        # create frame from the data
        if len(block_data) == 0:
            frame = None
        else:
            frame = self.context.frame.create(block_data,
                                              schema=schema)
        return frame

    def test_simple_line(self):
        """ Verify that SvmModel operates as expected.  """
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

        # create a frame from the training data
        training_frame = self.lattice2frame(train_lattice)

        # train the model on the data
        svm_model = self.context.models.classification.svm.train(training_frame,
                                                                 "model_class",
                                                                 ["x", "y"])

        # Test against the original training data;
        # this should match almost perfectly.
        predicted_frame = svm_model.predict(training_frame)

        # test the model, check that the accuracy is sufficient
        test_obj = svm_model.test(predicted_frame)
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0)

        # create a frame from the test data and predict
        outer_square = self.lattice2frame(test_lattice)
        svm_model.predict(outer_square, ["x", "y"])

        # test the model and check accuracy
        test_obj = svm_model.test(outer_square)
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0)

    def test_multi_dim(self):
        """ Verify that SvmModel operates as expected in 3 dimensions"""
        # Test set is a 3D model on the plane x + y - 2z > 0
        train_data = [
            (0, 0, 0, 1),
            (1, 1, 1, 1),
            (1, 1, 4, 0),
            (2, 3, 3, 0),
            (2, 3, 2, 1),
            (2, 0, 0, 1),
            (3, 0, 2, 0),
            (3, 6, 0, 1),
            (3, -4, 0, 0),
            (50, 50, 49, 1),
            (50, 50, 51, 0),
            (-50, -50, -49, 0),
            (-50, -50, -51, 1),
        ]

        test_data = [
            (0, 0, 1, 0),
            (-1, -1, 1, 0),
            (-1, -2, -1, 0),
            (100, 100, 95, 1),
            (-100, -100, -95, 0),
            (2, 2, 1, 1)
        ]

        schema = [('d1', float),
                  ('d2', float),
                  ('d3', float),
                  ('model_class', int),
                  ]
        # create a frame from the training data and train the model
        training_frame = self.context.frame.create(train_data,
                                                   schema=schema)
        svm_model = self.context.models.classification.svm.train(training_frame,
                                                                 "model_class",
                                                                 ["d1", "d2", "d3"])

        # Test against the original training data;
        # this should match almost perfectly.
        predicted_frame = svm_model.predict(training_frame)

        # test the model and verify accuracy
        test_obj = svm_model.test(predicted_frame)
        self.assertGreaterEqual(test_obj.accuracy, 0.75)

        # create a frame from the test data and predict
        outer_square = self.context.frame.create(
            test_data, schema=schema)
        predicted_frame2 = svm_model.predict(outer_square, ["d1", "d2", "d3"])

        # test the model and verify accuracy
        test_obj = svm_model.test(predicted_frame2)
        self.assertGreaterEqual(test_obj.accuracy, 0.75)

    def test_fuzzy_line(self):
        """ Verify that SvmModel operates as expected.  """
        # Test set is a lattice of points, a few outliers, large gap.
        train_lattice = ["++   --",
                         "++    +",
                         "++    -",
                         "+-+  --",
                         "++    -",
                         "+    --",
                         "++    -",
                         ]

        test_lattice = ["+++----",
                        "+++----",
                        "++++---",
                        "++++---",
                        "++++---",
                        "++++---",
                        "++++---",
                        ]

        training_frame = self.lattice2frame(train_lattice)
        svm_model = self.context.models.classification.svm.train(training_frame,
                                                                 "model_class",
                                                                 ["x", "y"])

        # Test against the original training data;
        # this should match almost perfectly.
        predicted_frame = svm_model.predict(training_frame)

        test_obj = svm_model.test(predicted_frame)
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0)

        outer_square = self.lattice2frame(test_lattice)
        predicted_frame2 = svm_model.predict(outer_square, ["x", "y"])

        test_obj = svm_model.test(predicted_frame2)

        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0)

    def test_pca_publish(self):
        """ validate publish"""
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

        svm_model = self.context.models.classification.svm.train(training_frame,
                                                                 "model_class",
                                                                 ["x", "y"])
        file_name = self.get_name("svm")
        path = svm_model.export_to_mar(self.get_export_file(file_name))

        self.assertIn("hdfs", path)
        self.assertIn("svm", path)

    def test_biased_line(self):
        """ Verify that SvmModel operates as expected"""
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

        svm_model = self.context.models.classification.svm.train(training_frame,
                                                                 "model_class",
                                                                 ["x", "y"])

        # Test against the original training data;
        # this should match almost perfectly.
        predicted_frame = svm_model.predict(training_frame)

        test_obj = svm_model.test(predicted_frame)
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0)

    def test_all_in_class(self):
        """ Verify that SvmModel operates as expected"""
        # Test set is a 3x3 square lattice of points, all in the model.
        # Validate that all other points are in the model.

        train_lattice = ["+++",
                         "+++",
                         "+++"]

        test_lattice = ["+                   +         +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+                             +",
                        "+              +              +"
                        ]

        training_frame = self.lattice2frame(train_lattice)

        svm_model = self.context.models.classification.svm.train(training_frame,
                                                                 "model_class",
                                                                 ["x", "y"])

        outer_square = self.lattice2frame(test_lattice)
        predicted_frame = svm_model.predict(outer_square, ["x", "y"])

        test_obj = svm_model.test(predicted_frame)
        self.assertGreaterEqual(test_obj.accuracy, 8.0/9.0)

    def test_shuttle(self):
        """ Test with Kathleen's 'shuttle scale cut' data """
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
        train_frame = self.context.frame.import_csv(self.shuttle_file_tr,
                                                    schema=[("input_line", str)])

        train_frame.add_columns(split_input_line,
                                self.shuttle_schema)
        train_frame.drop_columns("input_line")

        svm_model = self.context.models.classification.svm.train(train_frame,
                                                                 "class",
                                                                 observe_list)

        # Build testing frame and test the model
        test_frame = self.context.frame.import_csv(self.shuttle_file_te,
                                                   schema=[("input_line", str)])

        test_frame.add_columns(split_input_line,
                               self.shuttle_schema)
        test_frame.drop_columns("input_line")

        test_result = svm_model.test(test_frame)
        matrix = test_result.confusion_matrix
        tn = matrix['Predicted_Neg']['Actual_Neg']
        fp = matrix['Predicted_Pos']['Actual_Neg']
        fn = matrix['Predicted_Neg']['Actual_Pos']
        tp = matrix['Predicted_Pos']['Actual_Pos']
        total_row_count = tn + fp + fn + tp
        self.assertLess((fp+fn)/total_row_count, 0.01)

        # Build prediction frame and use the model to forecast
        pred_frame = self.context.frame.import_csv(self.shuttle_file_va,
                                                   schema=[("input_line", str)])

        pred_frame.add_columns(split_input_line,
                               self.shuttle_schema)
        pred_frame.drop_columns("input_line")

        predicted_frame = svm_model.predict(pred_frame, observe_list)
        self.assertLess(predicted_frame.count() / total_row_count, 0.01)

    def test_3class_linear(self):
        """ Verify that 3-class model raises an exception.  """
        with self.assertRaisesRegexp(Exception, "Input validation failed"):
            self.context.models.classification.svm.train(self.training_frame,
                                                         "model_class",
                                                         ["x", "y"])

    def test_bad_class_column(self):
        """Try a bad classification column"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.context.models.classification.svm.train(self.training_frame,
                                                         "no_such_col",
                                                         ["x", "y"])

    def test_bad_data_column(self):
        """Try a bad data column"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.context.models.classification.svm.train(
                self.training_frame, "model_class", ["no_such_col", "y"])

    def test_null_column(self):
        """Try a null data column"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            self.context.models.classification.svm.train(self.training_frame,
                                                         "model_class",
                                                         ["x", None])

    def test_bad_data(self):
        """ Verify that invalid models raise exceptions.  """
        # Empty training set

        train_lattice = ["1  1  1  1",
                         "          ",
                         "1  1  10  ",
                         "1         ",
                         "         0",
                         "0        0"]

        training_frame = self.lattice2frame(train_lattice)
        training_frame.drop_rows(lambda row: True)

        with self.assertRaisesRegexp(Exception, "Frame is empty"):
            self.context.models.classification.svm.train(training_frame,
                                                         "model_class",
                                                         ["x", "y"])


if __name__ == "__main__":
    unittest.main()
