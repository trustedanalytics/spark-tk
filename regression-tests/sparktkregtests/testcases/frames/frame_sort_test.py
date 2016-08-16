"""Test interface functionality of frame.sort"""

import unittest
import sys
from sparktkregtests.lib import sparktk_test


class FrameSortTest(sparktk_test.SparkTKTestCase):
    """Test fixture the Frame sort function"""

    def setUp(self):
        super(FrameSortTest, self).setUp()

        # create standard, defunct, and empty frames"
        dataset = self.get_file("dogs.csv")
        schema = [("age", int),
                  ("name", str),
                  ("owner", str),
                  ("weight", int),
                  ("hair_type", str)]
        # header=True means first line will be skipped
        self.frame = self.context.frame.import_csv(dataset,
                schema=schema, header=True)

    def test_frame_sort_single_column_ascending(self):
        """ Test single-column sorting ascending"""
        # sort by weight, defaults to ascending
        self.frame.sort("weight")
        # get just the weight column
        sorted = self.frame.copy("weight")
        # take all rows and examine data
        sorted_data = sorted.take(sys.maxint).data
        # last will keep track of the previous item
        # we initialize it to an integer minimum
        last = -1 * sys.maxint
        # iterate through and assert that the last item
        # is less than current while updating current
        for i in range(len(sorted_data)):
            assert sorted_data[i][0] >= last
            last = sorted_data[i][0]

    # this test will instead of comparing just the weight column
    # it will make sure row integrity is preserved across all cols
    # as a sanity check that the algorithm is not just sorting
    # the weight column but is in fact sorting all of the rows by
    # the weight column value
    def test_frame_sort_single_column_ascending_compare_all_cols(self):
        """ Test single-column sorting ascending with the argument"""
        frame_copy = self.frame.copy()
        unsorted_data = frame_copy.take(frame_copy.row_count).data
        self.frame.sort("weight", ascending=True)
        sorted = self.frame.copy()
        sorted_data = sorted.take(sorted.row_count).data
        last = -1 * sys.maxint
        for i in range(len(sorted_data)):
            assert sorted_data[i][3] >= last
            last = sorted_data[i][3]
            # here we are making sure that the row integrity is
            # preserved by checking that the entire row
            # exists as is in the original data
            if sorted_data[i] not in unsorted_data:
                raise ValueError("integrity of row not preserved through sorting")

    def test_frame_sort_single_column_descending(self):
        """ Test single-column sorting descending with the argument"""
        self.frame.sort("weight", ascending=False)
        sorted = self.frame.copy("weight")
        sorted_data = sorted.take(sys.maxint).data
        last = sys.maxint
        for i in range(len(sorted_data)):
            assert sorted_data[i][0] <= last
            last = sorted_data[i][0]

    def test_frame_sort_multiple_column_ascending(self):
        """ Test multiple-column sorting ascending"""
        unsorted = self.frame.download(self.frame.row_count)
        self.frame.sort(["weight", "hair_type"])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = unsorted.sort_values(['weight', 'hair_type'])
        # compare the data we sorted with the sorted frame
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])

    @unittest.skip("frame.sort does not allow tuples")
    def test_frame_sort_multiple_column_tuple_descending(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort([("weight", False), ("hair_type", False)])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(['weight', 'hair_type'],
                ascending=[False, False])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])

    @unittest.skip("frame.sort allows ascending param to be an array, does not work as expected")
    def test_frame_sort_multiple_column_descending(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort(['weight', 'hair_type'], ascending=[False, False])
        up_take = self.frame.download(self.frame.row_count)
        sorted_vals = up_take.sort_values(['weight', 'hair_type'],
                ascending=[False, False])
        for i in range(len(sorted_vals)):
            self.assertEqual(
                up_take.iloc[i]['weight'], sorted_vals.iloc[i]['weight'])
            self.assertEqual(
                up_take.iloc[i]['hair_type'], sorted_vals.iloc[i]['hair_type'])

    def test_frame_sort_multiple_column_mixed(self):
        """ Test multiple-column sorting descending with the argument"""
        self.frame.sort([("weight", False), ("hair_type", True), ('age', True)])
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
        with self.assertRaisesRegexp(Exception, "Invalid column"):
            self.frame.sort('no-such-column')

    def test_invalid_arguments(self):
        """Test no arguments errors"""
        with self.assertRaisesRegexp(Exception, "2 arguments"):
            self.frame.sort()


if __name__ == "__main__":
    unittest.main()
