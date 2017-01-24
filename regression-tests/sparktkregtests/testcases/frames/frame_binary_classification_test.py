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

""" tests multiclass classification metrics"""

import unittest
from sparktkregtests.lib import sparktk_test


class BinaryClassificationMetrics(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Tests binary classification"""
        super(BinaryClassificationMetrics, self).setUp()
        self.dataset = [("blue", 1, 0, 0),
                        ("blue", 3, 1, 0),
                        ("green", 1, 0, 0),
                        ("green", 0, 1, 0)]
        self.schema = [("a", str),
                       ("b", int),
                       ("labels", int),
                       ("predictions", int)]

        self.frame = self.context.frame.create(self.dataset,
                                               schema=self.schema)

    def test_binary_classification_metrics(self):
        """test binary classification metrics with normal data"""
        # call the binary classification metrics function
        class_metrics = self.frame.binary_classification_metrics(
            "labels", "predictions", 1, 1)
        # get the confusion matrix values
        conf_matrix = class_metrics.confusion_matrix.values
        # labeling each of the cells in our confusion matrix
        # makes this easier for me to read
        # the confusion matrix should look something like this:
        #            predicted pos       predicted neg
        # actual pos    [0][0]              [0][1]
        # actual neg    [1][0]              [1][1]
        true_pos = conf_matrix[0][0]
        false_neg = conf_matrix[0][1]
        false_pos = conf_matrix[1][0]
        true_neg = conf_matrix[1][1]
        # the total number of predictions, total number pos and neg
        total_pos = true_pos + false_neg
        total_neg = true_neg + false_pos
        total = total_pos + total_neg

        # recall is defined in the docs as the total number of true pos
        # results divided by the false negatives + pos
        recall = true_pos / (false_neg + true_pos)
        # from the docs, precision = true pos / false pos + true pos
        precision = true_pos / (false_pos + true_pos)
        # from the docs this is the def of f_measure
        f_measure = (recall * precision) / (recall + precision)
        # according to the documentation the accuracy
        # is defined as the total correct predictions divided by the
        # total number of predictions
        accuracy = float(true_pos + true_neg) / float(total)
        pos_count = 0
        pandas_frame = self.frame.to_pandas()
        # calculate the number of pos results and neg results in the data
        for index, row in pandas_frame.iterrows():
            if row["labels"] is 1:
                pos_count = pos_count + 1
        neg_count = total - pos_count

        # finally we compare our results with sparktk's
        self.assertAlmostEqual(class_metrics.recall, recall)
        self.assertAlmostEqual(class_metrics.precision, precision)
        self.assertAlmostEqual(class_metrics.f_measure, f_measure)
        self.assertAlmostEqual(class_metrics.accuracy, accuracy)
        self.assertEqual(total_pos, pos_count)
        self.assertEqual(total_neg, neg_count)

    def test_binary_classification_metrics_bad_beta(self):
        """Test binary classification metrics with negative beta"""
        # should throw an error because beta must be >0
        with self.assertRaisesRegexp(Exception, "greater than or equal to 0"):
            self.frame.binary_classification_metrics(
                "labels", "predictions", 1, beta=-1)

    def test_binary_classification_metrics_valid_beta(self):
        """test binary class metrics with a valid value for beta"""
        # this is a valid value for beta so this should not throw an error
        self.frame.binary_classification_metrics(
            "labels", "predictions", 1, beta=2)

    def test_binary_classification_matrics_with_invalid_beta_type(self):
        """Test binary class metrics with a beta of invalid type"""
        with self.assertRaisesRegexp(
                Exception, "could not convert string to float"):
            self.frame.binary_classification_metrics(
                "labels", "predictions", 1, beta="bla")

    def test_binary_classification_metrics_with_invalid_pos_label(self):
        """Test binary class metrics with a pos label that does not exist"""
        # should not error but should return no pos predictions
        class_metrics = self.frame.binary_classification_metrics(
            "labels", "predictions", "bla", 1)

        # assert that no positive results were found since
        # there are no labels in the data with "bla"
        conf_matrix = class_metrics.confusion_matrix.values
        # assert no predicted pos actual pos
        self.assertEqual(conf_matrix[0][0], 0)
        # assert no actual pos predicted neg
        self.assertEqual(conf_matrix[1][0], 0)

    def test_binary_classification_metrics_with_frequency_col(self):
        """test binay class metrics with a frequency column"""
        dataset = [("blue", 1, 0, 0, 1),
                   ("blue", 3, 1, 0, 1),
                   ("green", 1, 0, 0, 3),
                   ("green", 0, 1, 0, 1)]
        schema = [("a", str),
                  ("b", int),
                  ("labels", int),
                  ("predictions", int),
                  ("frequency", int)]
        frame = self.context.frame.create(dataset, schema=schema)

        class_metrics = frame.binary_classification_metrics(
            "labels", "predictions", 1, 1, frequency_column="frequency")

        conf_matrix = class_metrics.confusion_matrix.values

        true_pos = conf_matrix[0][0]
        false_neg = conf_matrix[0][1]
        false_pos = conf_matrix[1][0]
        true_neg = conf_matrix[1][1]
        total_pos = true_pos + false_neg
        total_neg = true_neg + false_pos
        total = total_pos + total_neg

        # these calculations use the definitions from the docs
        recall = true_pos / (false_neg + true_pos)
        precision = true_pos / (false_pos + true_pos)
        f_measure = (recall * precision) / (recall + precision)
        accuracy = float(true_pos + true_neg) / float(total)
        pos_count = 0
        pandas_frame = self.frame.to_pandas()
        # calculate the number of pos results and neg results in the data
        for index, row in pandas_frame.iterrows():
            if row["labels"] is 1:
                pos_count = pos_count + 1
        neg_count = total - pos_count

        # finally we check that our values match sparktk's
        self.assertAlmostEqual(class_metrics.recall, recall)
        self.assertAlmostEqual(class_metrics.precision, precision)
        self.assertAlmostEqual(class_metrics.f_measure, f_measure)
        self.assertAlmostEqual(class_metrics.accuracy, accuracy)
        self.assertEqual(total_pos, pos_count)
        self.assertEqual(total_neg, neg_count)

    def test_binary_classification_metrics_with_invalid_frequency_col(self):
        """test binary class metrics with a frequency col of invalid type"""
        dataset = [("blue", 1, 0, 0, "bla"),
                   ("blue", 3, 1, 0, "bla"),
                   ("green", 1, 0, 0, "bla"),
                   ("green", 0, 1, 0, "bla")]
        schema = [("a", str),
                  ("b", int),
                  ("labels", int),
                  ("predictions", int),
                  ("frequency", str)]

        frame = self.context.frame.create(dataset, schema=schema)

        # this should throw an error because the frequency col
        # we provided is of type str but should be of type int
        with self.assertRaisesRegexp(Exception, "NumberFormatException"):
            frame.binary_classification_metrics(
                "labels", "predictions", 1, 1, frequency_column="frequency")


if __name__ == '__main__':
    unittest.main()
