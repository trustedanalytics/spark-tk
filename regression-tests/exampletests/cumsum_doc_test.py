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

""" THIS TEST REQUIRES NO THIRD PARTY APPLICATIONS OTHER THAN THE SPARKTK
    THIS TEST IS TO BE MAINTAINED AS A SMOKE TEST FOR THE ML SYSTEM
"""
import unittest
from sparktk import TkContext


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
        frame = tc.frame.create(data=[[0, 100],
                                      [3, 20],
                                      [1, 25],
                                      [2, 90]],
                                schema=[("order", int),
                                        ("value", int)])
        print frame.inspect()

        # Sort on order, note this is a side effect based operation
        frame.sort('order')

        # calculate the cumulative sum
        frame.cumulative_sum('value')
        print frame.inspect()

        # Fetch the results, and validate they are what you would expect
        result = frame.take(frame.count())
        self.assertItemsEqual(
            result, [[0, 100, 100],
                     [3, 20, 235],
                     [1, 25, 125],
                     [2, 90, 215]])

if __name__ == '__main__':
    unittest.main()
