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
"""Test interface functionality of frame.sort"""
import unittest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

class FrameSortTest(sparktk_test.SparkTKTestCase):
    """Test fixture the Frame sort function"""
    frame = None

    def setUp(self):
        super(FrameSortTest, self).setUp()

        # create standard, defunct, and empty frames"
        dataset = self.get_file("movie.csv")
        schema = [("src", int),
                  ("dir", str),
                  ("dest", int),
                  ("weight", int),
                  ("e_type", str)]
        self.frame = self.context.frame.import_csv(dataset, schema=schema, header=True) # creates and returns a frame from a csv file, header=True means first line will be skipped

    def test_frame_sort_single_column_ascending(self):
        """ Test single-column sorting ascending"""
        self.frame.sort("weight") # sort our data by weight, defaults to ascending
        sorted = self.frame.copy("weight") # gets just the weight column and sets sorted to that
        sorted_data = sorted.take(sys.maxint).data # take all of the rows and look at just the data
        last = -1 * sys.maxint # last will keep track of the previous item in the list, we will initialize it to an integer minimum
        for i in range(len(sorted_data)): # iterate trough the sorted data
            assert sorted_data[i][0] >= last # assert that the last item is lower than the current item#
            last = sorted_data[i][0] # set last to the current item before advancing

    def test_frame_sort_single_column_ascending_no_default(self):
        """ Test single-column sorting ascending with the argument"""
        self.frame.sort("weight", ascending=True) # sort our data by weight, defaults to ascending
        sorted = self.frame.copy("weight") # gets just the weight column and sets sorted to that
        sorted_data = sorted.take(sys.maxint).data # take all of the rows and look at just the data
        last = -1 * sys.maxint # last will keep track of the previous item in the list, we will initialize it to an integer minimum
        for i in range(len(sorted_data)): # iterate trough the sorted data
            assert sorted_data[i][0] >= last # assert that the last item is lower than the current item
            last = sorted_data[i][0] # set last to the current item before advancing
              
    def test_frame_sort_single_column_descending(self):
        """ Test single-column sorting descending with the argument"""
        self.frame.sort("weight", ascending=False) # sort our data by weight, defaults to ascending
        sorted = self.frame.copy("weight") # gets just the weight column and sets sorted to that
        sorted_data = sorted.take(sys.maxint).data # take all of the rows and look at just the data
        last = sys.maxint # last will keep track of the previous item in the list, we will initialize it to an integer minimum
        for i in range(len(sorted_data)): # iterate trough the sorted data
            assert sorted_data[i][0] <= last # assert that the last item is lower than the current item
            last = sorted_data[i][0] # set last to the current item before advancing
    
    def test_frame_sort_multiple_column_ascending(self):
        """ Test multiple-column sorting ascending"""
        unsorted = self.frame.download(self.frame.row_count)
        self.frame.sort(["weight", "e_type"])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = unsorted.sort_values(['weight', 'e_type'])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['e_type'], sorted_vals.iloc[i]['e_type'])
    
    def test_frame_sort_multiple_column_tuple_descending(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort([("weight", False), ("e_type", False)])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(['weight', 'e_type'], ascending=[False, False])
        print "frame.sort: " + str(self.frame.inspect())
        print "sorted vals: " + str(sorted_vals)
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['e_type'], sorted_vals.iloc[i]['e_type'])   

    def test_frame_sort_multiple_column_descending(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort(['weight', 'e_type'], ascending=[False, False])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(['weight', 'e_type'], ascending=[False, False])
        print "frame.sort: " + str(self.frame.inspect())
        print "sorted vals: " + str(sorted_vals)
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['e_type'], sorted_vals.iloc[i]['e_type'])

    def test_frame_sort_multiple_column_mixed(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort([("weight", False), ("e_type", True), ('src', True)])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(
            ['weight', 'e_type', 'src'], ascending=[False, True, True])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['e_type'], sorted_vals.iloc[i]['e_type'])
            self.assertEqual(
                up_take.iloc[i]['src'], sorted_vals.iloc[i]['src'])

    def test_frame_sort_error_bad_column(self):
        """ Test error on non-existant column"""
        with self.assertRaises(Exception):
            self.frame.sort('no-such-column')

    def test_invalid_arguments(self):
        """Test no arguments errors"""
        with self.assertRaises(Exception):
            self.frame.sort()

if __name__ == "__main__":
    unittest.main()
