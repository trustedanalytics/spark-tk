""" test cases for the confusion matrix
    usage: python2.7 confusion_matrix_test.py

    Tests the confusion matrix functionality against known values. Confusion
    matrix is returned as a pandas frame.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import unittest

from sparktk import TkContext
from qalib import sparktk_test


class ConfusionMatrix(sparktk_test.SparkTKTestCase):


    def test_confusion_matrix_for_data_33_55(self):
        """Verify the input and baselines exist before running the tests."""
        super(ConfusionMatrix, self).setUp()
        self.schema = [("value", int),
                        ("predicted", int)]
        perf = self.get_file("classification_metrics.csv") # our data file in qa_data
        self.actual_result = [64, 15, 23, 96] # what we expect to get from the confusion matrix
        frame = self.context.frame.import_csv(perf, schema=self.schema) # imports our data and returns a frame
        classMetrics = frame.binary_classification_metrics('value', 'predicted', 1, 1) # params are label column, result column, pos column
        confMatrix = classMetrics.confusion_matrix.values 
        cumulative_matrix_list = [confMatrix[0][0], confMatrix[0][1], confMatrix[1][0], confMatrix[1][1]]
        self.assertEqual(self.actual_result, cumulative_matrix_list) # compare our confusion matrix values with the expected values


if __name__ == '__main__':
    unittest.main()
