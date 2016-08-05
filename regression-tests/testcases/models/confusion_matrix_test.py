"""Tests the confusion matrix functionality against known values"""
import unittest

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from qalib import sparktk_test


class ConfusionMatrix(sparktk_test.SparkTKTestCase):

    def test_confusion_matrix(self):
        """Tests the confusion matrix functionality"""
        perf = self.get_file("classification_metrics.csv")
        schema = [("value", int), ("predicted", int)]
        # [true_positive, false_negative, false_positive, true_negative]
        actual_result = [64, 15, 23, 96]

        frame = self.context.frame.import_csv(perf, schema=schema)

        cm = frame.binary_classification_metrics('value', 'predicted', 1, 1)

        conf_matrix = cm.confusion_matrix.values
        cumulative_matrix_list = [conf_matrix[0][0],
                                  conf_matrix[0][1],
                                  conf_matrix[1][0],
                                  conf_matrix[1][1]]
        self.assertEqual(actual_result, cumulative_matrix_list)

if __name__ == '__main__':
    unittest.main()
