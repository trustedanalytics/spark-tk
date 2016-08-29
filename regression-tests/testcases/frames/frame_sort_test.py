"""Test interface functionality of frame.sort"""
import unittest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

# related bug tickets:
#@DPNG-9407 frame sort does not allow tuples
#@DPNG-9405 frame sort allows ascending parameter to be a list
#@DPNG-9401 import_csv allows the user to set inferschema to true and also provide a schema
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

class FrameSortTest(sparktk_test.SparkTKTestCase):
    """Test fixture the Frame sort function"""
    frame = None

    def setUp(self):
        super(FrameSortTest, self).setUp()

        # create standard, defunct, and empty frames"
        dataset = self.get_file("dogs.csv")
        schema = [("age", int),
                  ("name", str),
                  ("owner", str),
                  ("weight", int),
                  ("hair_type", str)]
        self.frame = self.context.frame.import_csv(dataset, schema=schema, header=True) # header=True means first line will be skipped

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
        self.frame.sort(["weight", "hair_type"])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = unsorted.sort_values(['weight', 'hair_type'])
        for i in range(len(sorted_vals)): # compare the data we sorted with the sorted frame
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])
    
    def test_frame_sort_multiple_column_tuple_descending(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort([("weight", False), ("hair_type", False)]) # these tests will fail currently because of a bug (see above)
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(['weight', 'hair_type'], ascending=[False, False])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])   

    def test_frame_sort_multiple_column_descending(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort(['weight', 'hair_type'], ascending=[False, False])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(['weight', 'hair_type'], ascending=[False, False])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])

    def test_frame_sort_multiple_column_mixed(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort([("weight", False), ("hair_type", True), ('age', True)]) # See bug DPNG-9407
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(
            ['weight', 'hair_type', 'age'], ascending=[False, True, True])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])
            self.assertEqual(
                up_take.iloc[i]['age'], sorted_vals.iloc[i]['age'])

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
