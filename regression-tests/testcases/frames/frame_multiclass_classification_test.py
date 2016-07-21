""" tests multiclass classification metrics"""

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class ClassificationMetrics(sparktk_test.SparkTKTestCase):


    def test_multinomial_frame_classification(self):
        """Tests the confusion matrix functionality"""
        schema = [("actual", int), ("predicted", int), ("count", int)]
        class_csv = self.get_file("class_data.csv") # the name of our data file, needs to be in hdfs for the test to run
        actual_result = [5, 2, 3, 0, 5, 3, 0, 2, 5] # the values we expect to get from our test
        frame = self.context.frame.import_csv(class_csv, schema=schema) # uploading our data file, will return a frame
        classMetrics = frame.multiclass_classification_metrics('actual', 'predicted', frequency_column='count') # the label column, result column, and frequency column are the params
        conf_matrix = classMetrics.confusion_matrix.values 
        cumulative_matrix_list = [conf_matrix[0][0],
                                  conf_matrix[0][1],
                                  conf_matrix[0][2],
                                  conf_matrix[1][0],
                                  conf_matrix[1][1],
                                  conf_matrix[1][2],
                                  conf_matrix[2][0],
                                  conf_matrix[2][1],
                                  conf_matrix[2][2]]
        self.assertEqual(actual_result, cumulative_matrix_list)

if __name__ == '__main__':
    unittest.main()
