##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
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
""" usage: python2.7 naive_bayes_doc_test.py

    THIS TEST REQUIRES NO THIRD PARTY APPLICATIONS OTHER THAN THE ATK
    THIS TEST IS TO BE MAINTAINED AS A SMOKE TEST FOR THE ML SYSTEM
"""
import unittest
from sparktk import TkContext
from sparktk import dtypes

class CumSumTest(unittest.TestCase):

    def test_frame_basic(self):
        """Documentation test for classifiers"""

        # The general workflow will be build a frame, run some analytics
        # on the frame

        # First Step, construct a frame
        # Construct a frame to be uploaded, this is done using plain python
        # lists uploaded to the server

        # The following frame could represent some ordered list (such as
        # customer orders) and a value associated with the order.
        # The order is sorted on, and then the order value is accumulated

        # Cumulative sum finds the sum up to and including a given order
	
        # Create context 
        tc = TkContext()


        # Create the frame using a list object
        frame = tc.frame.create([[0,100],
                                    [3,20],
                                    [1,25],
                                    [2,90]],
                                   ["order",
                                    "value"])

        print frame.inspect()

        # Sort on order, note this is a side effect based operation

        frame.sort('order')

        # calculate the cumulative sum

        frame.cumulative_sum('value')
        
        print frame.inspect()

        # Fetch the results, and validate they are what you would expect

        result = frame.take(frame.row_count)
        self.assertItemsEqual(
            result.data, [[0,100,100], [3,20,235], [1,25,125], [2,90,215]])


if __name__ == '__main__':
    unittest.main()


