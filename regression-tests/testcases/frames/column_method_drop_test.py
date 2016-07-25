"""Tests methods that access or alter columns"""
import unittest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from qalib import sparktk_test

dummy_int_val = -77     # placeholder data value for added column
dummy_col_count = 1000  # length of dummy list for column add


# This method is to test different sources of functions
# i.e. global
def global_dummy_val_list(row):
    return [dummy_int_val for _ in range(0, dummy_col_count)]


class ColumnMethodTest(sparktk_test.SparkTKTestCase):

    # Test class bound methods
    @staticmethod
    def static_dummy_val_list(row):
        return [dummy_int_val for _ in range(0, dummy_col_count)]

    def setUp(self):
        """Build test_frame"""
        super(ColumnMethodTest, self).setUp()
        dataset = self.get_file("int_str_float.csv")
        schema = [("int", int), ("str", str), ("float", float)]

        self.frame = self.context.frame.import_csv(dataset, schema=schema)

    def test_column_names(self):
        """all original columns"""
        header = self.frame.column_names
        self.assertEqual(header, ['int', 'str', 'float'])

    def test_column_names_drop(self):
        """Exercise subsets of 1 and 2 columns"""
        self.frame.drop_columns('str')
        header = self.frame.column_names
        self.assertEqual(header, ['int', 'float'])

    def test_column_names_drop_multiple(self):
        """Drop multiple columns"""
        self.frame.drop_columns(['str', 'float'])
        header = self.frame.column_names
        self.assertEqual(header, ['int'])

    def test_drop_non_existent_column(self):
        """test dropping non-existent column"""
        with self.assertRaises(ValueError):
            self.frame.drop_columns("non-existent")

    def test_drop_columns(self):
        """Test drop columns scenarios"""
        self.frame.add_columns(
            lambda row: dummy_int_val, ('product', int))

        col_count = len(self.frame.take(1)[0][0])
        self.frame.drop_columns(['int'])
        self.assertNotIn('int', self.frame.column_names)
        self.assertEqual(col_count-1, len(self.frame.take(1)[0][0]))

    def test_drop_columns_multiple(self):
        """Test drop columns multiple, repeated"""
        self.frame.add_columns(
            lambda row: dummy_int_val, ('product', int))

        col_count = len(self.frame.take(1)[0][0])
        self.frame.drop_columns(['str', 'product', 'str'])
        self.assertNotIn('str', self.frame.column_names)
        self.assertNotIn('product', self.frame.column_names)
        self.assertEqual(col_count-2, len(self.frame.take(1)[0][0]))

    def test_drop_zero_columns(self):
        """Test dropping no columns"""
        with self.assertRaises(ValueError):
            self.frame.drop_columns([])

    def test_drop_nonexistent_column(self):
        """Test drop non-existent column"""
        with self.assertRaises(ValueError):
            self.frame.drop_columns(['no-such-name'])

if __name__ == "__main__":
    unittest.main()
