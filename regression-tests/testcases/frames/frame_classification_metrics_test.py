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
""" test multinomial and count confusion matrix against known values"""

import unittest

#import trustedanalytics as ia

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from qalib import common_utils as cu
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
