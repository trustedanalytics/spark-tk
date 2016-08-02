""" Tests that flatten and unflatten work in multi-column mode """
import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.realpath(__file__)))))
from qalib import sparktk_test
from sparktk import dtypes


class FlattenUnflatten(sparktk_test.SparkTKTestCase):

    def test_flatten_unflatten_basic(self):
        """ test for flatten/unflatten comma-separated rows """
        # file contains 11 rows
        schema_flatten = [("col_A", int),
                          ("col_B", str),
                          ("col_C", int)]
        self.frame = self.context.frame.import_csv(
            self.get_file("stackedNums.csv"), schema=schema_flatten)

        original_copy = self.frame.copy()
        row_count = self.frame.row_count
        self.assertEqual(row_count, 11)
        # check that flatten into unflatten retuns the same original frame
        self.frame.flatten_columns('col_B')
        flat_copy = self.frame.copy()
        self.frame.unflatten_columns(['col_A'])
        c1 = original_copy.copy(['col_A', 'col_B'])
        c2 = self.frame.copy(['col_A', 'col_B'])
        c1.sort('col_A')
        c2.sort('col_A')
        self.assertFramesEqual(c1, c2)
        # flatten again, see that is equivalent to flattening first time
        self.frame.flatten_columns('col_B')
        reflat_copy = self.frame.copy()
        self.assertFramesEqual(
            reflat_copy.copy(['col_A', 'col_B']),
            flat_copy.copy(['col_A', 'col_B']))

    def test_flatten_mult_simple(self):
        """Test multiple columns flatten"""
        block_data = [
            [[4, 3, 2, 1],
             "Calling cards,French toast,Turtle necks,Partridge in a Parody"],
            [[8, 7, 6, 5],
             "Maids a-milking,Swans a-swimming,Geese a-laying,Gold rings"],
            [[12, 11, 10, 9],
             "Drummers drumming,Lords a-leaping,Pipers piping,Ladies dancing"]]
        block_schema = [("day", dtypes.vector(4)), ("gift", str)]
        expected_take = [[12, "Drummers drumming"],
                         [11, "Lords a-leaping"],
                         [10, "Pipers piping"],
                         [9, "Ladies dancing"],
                         [8, "Maids a-milking"],
                         [7, "Swans a-swimming"],
                         [6, "Geese a-laying"],
                         [5, "Gold rings"],
                         [4, "Calling cards"],
                         [3, "French toast"],
                         [2, "Turtle necks"],
                         [1, "Partridge in a Parody"]]
        frame = self.context.frame.create(block_data, schema=block_schema)
        # Validate flatten against hand crafted results
        frame.flatten_columns("day", "gift")
        frame_take = frame.take(frame.row_count).data
        self.assertItemsEqual(frame_take, expected_take)

    def test_flatten_vary_len(self):
        """Test vector flatten"""
        block_data = [["1,2,3", "a,b,c,d"]]
        expected_take = [['1', 'a'], ['2', 'b'], ['3', 'c'], [None, 'd']]
        block_schema = [("col1", str), ("col2", str)]
        frame = self.context.frame.create(block_data, schema=block_schema)
        # Validate flatten on known results
        frame.flatten_columns('col1', 'col2')
        frame_take = frame.take(frame.row_count).data
        self.assertItemsEqual(frame_take, expected_take)

    def test_flatten_delim_variety(self):
        """Test flatten fails on invalid delimiter"""
        block_data = [["a b c", "d/e/f", "10,18,20"],
                      ["g h", "i/j", "4,5"],
                      ["k l", "m/n", "13"],
                      ["o", "p/q", "7,25,6"]]
        block_schema = [("col1", str), ("col2", str), ("col3", str)]
        frame = self.context.frame.create(block_data, schema=block_schema)
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            frame.flatten_columns(["col1", "col2", "col3"], [' ', '/'])

    def test_flatten_delim_same(self):
        """Test flatten with custom delimiter"""
        block_data = [["a=b=c", "d=e=f", "10=18=20"],
                      ["g=h", "i=j", "4=5"],
                      ["k=l", "m=n", "13,13"],
                      ["o", "p=q", "7=25=6"]]
        block_schema = [("col1", str), ("col2", str), ("col3", str)]
        expected_flat = [[None, None, "6"],
                         [None, "q", "25"],
                         ["a", "d", "10"],
                         ["b", "e", "18"],
                         ["c", "f", "20"],
                         ["g", "i", "4"],
                         ["h", "j", "5"],
                         ["k", "m", "13,13"],
                         ["l", "n", None],
                         ["o", "p", "7"]]
        frame = self.context.frame.create(block_data, schema=block_schema)
        frame.flatten_columns(('col1', '='), ('col2', '='), ('col3', '='))
        actual_flat = frame.take(frame.row_count).data
        self.assertItemsEqual(expected_flat, actual_flat)

    def test_flatten_mult_example(self):
        """Test flatten on multiple columns"""
        block_data = [["a,b,c", "d,e,f", "10,18,20"],
                      ["g,h", "i,j", "4,5"],
                      ["k,l", "m,n", "13"],
                      ["o", "p,q", "7,25,6"]]
        block_schema = [("col1", str), ("col2", str), ("col3", str)]
        expected_flat = [[None, None, "6"],
                         [None, "q", "25"],
                         ["a", "d", "10"],
                         ["b", "e", "18"],
                         ["c", "f", "20"],
                         ["g", "i", "4"],
                         ["h", "j", "5"],
                         ["k", "m", "13"],
                         ["l", "n", None],
                         ["o", "p", "7"]]
        frame1 = self.context.frame.create(block_data, schema=block_schema)
        frame = frame1.copy()
        frame1.flatten_columns("col3")
        self.assertEqual(9, frame1.row_count)

        frame.flatten_columns("col1", "col2", "col3")
        actual_flat = frame.take(frame.row_count).data
        self.assertItemsEqual(expected_flat, actual_flat)

        # Unflatten should do nothing: there are no common elements.
        frame.unflatten_columns(["col1", "col2", "col3"])
        actual_unflat = frame.take(frame.row_count).data
        self.assertItemsEqual(expected_flat, actual_unflat)

    def test_flatten_int_column(self):
        """Test flatten on integer column"""
        block_data = [[1, "a,b,c"],
                      [2, "d,e,f"],
                      [3, "g,h,i"]]
        block_schema = [("col1", int), ("col2", str)]
        frame = self.context.frame.create(block_data, schema=block_schema)
        with self.assertRaisesRegexp(Exception, "Invalid column"):
            frame.flatten_columns("col1")

if __name__ == "__main__":
    unittest.main()
