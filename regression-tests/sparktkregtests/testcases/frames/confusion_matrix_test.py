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

"""Tests the confusion matrix functionality against known values"""
import unittest

from sparktkregtests.lib import sparktk_test


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
