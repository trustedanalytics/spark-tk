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


class ClassificationMetrics(sparktk_test.SparkTKTestCase):

    def test_multinomial_frame_classification(self):
        """Tests the confusion matrix functionality"""
        schema = [("actual", int), ("predicted", int), ("count", int)]
        # the name of our data file, needs to be in hdfs for the test to run
        class_csv = self.get_file("class_data.csv")
        # uploading our data file, will return a frame
        frame = self.context.frame.import_csv(class_csv, schema=schema)

        # the values we expect to get from our test
        actual_result = [5, 2, 3, 0, 5, 3, 0, 2, 5]

        # the label column, result column, and frequency column are the params
        classMetrics = frame.multiclass_classification_metrics(
            'actual', 'predicted', frequency_column='count')
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
